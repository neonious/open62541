/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 *
 *    Copyright 2015-2018 (c) Fraunhofer IOSB (Author: Julius Pfrommer)
 *    Copyright 2015 (c) Oleksiy Vasylyev
 *    Copyright 2016 (c) Sten Grüner
 *    Copyright 2017-2018 (c) Thomas Stalder, Blue Time Concept SA
 *    Copyright 2016-2017 (c) Florian Palm
 *    Copyright 2017 (c) Frank Meerkötter
 *    Copyright 2017 (c) Stefan Profanter, fortiss GmbH
 */

#include <open62541/client_highlevel.h>
#include <open62541/client_highlevel_async.h>

#include "ua_client_internal.h"

#ifdef UA_ENABLE_SUBSCRIPTIONS /* conditional compilation */

/*****************/
/* Subscriptions */
/*****************/

// Removed, because all of this was sync

/******************/
/* MonitoredItems */
/******************/

// Same thing, removed because all of this was sync

/*************************************/
/* Async Processing of Notifications */
/*************************************/

/* Assume the request is already initialized */
UA_StatusCode
UA_Client_preparePublishRequest(UA_Client *client, UA_PublishRequest *request) {
    /* Count acks */
    UA_Client_NotificationsAckNumber *ack;
    LIST_FOREACH(ack, &client->pendingNotificationsAcks, listEntry)
        ++request->subscriptionAcknowledgementsSize;

    /* Create the array. Returns a sentinel pointer if the length is zero. */
    request->subscriptionAcknowledgements = (UA_SubscriptionAcknowledgement*)
        UA_Array_new(request->subscriptionAcknowledgementsSize,
                     &UA_TYPES[UA_TYPES_SUBSCRIPTIONACKNOWLEDGEMENT]);
    if(!request->subscriptionAcknowledgements) {
        request->subscriptionAcknowledgementsSize = 0;
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }

    size_t i = 0;
    UA_Client_NotificationsAckNumber *ack_tmp;
    LIST_FOREACH_SAFE(ack, &client->pendingNotificationsAcks, listEntry, ack_tmp) {
        request->subscriptionAcknowledgements[i].sequenceNumber = ack->subAck.sequenceNumber;
        ++i;
        LIST_REMOVE(ack, listEntry);
        UA_free(ack);
    }
    return UA_STATUSCODE_GOOD;
}

void
UA_Client_Subscriptions_processPublishResponse(UA_Client *client, UA_PublishRequest *request,
                                               UA_PublishResponse *response) {
    UA_NotificationMessage *msg = &response->notificationMessage;

    client->currentlyOutStandingPublishRequests--;

    if(response->responseHeader.serviceResult == UA_STATUSCODE_BADTOOMANYPUBLISHREQUESTS) {
        if(client->config.outStandingPublishRequests > 1) {
            client->config.outStandingPublishRequests--;
            UA_LOG_WARNING(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                          "Too many publishrequest, reduce outStandingPublishRequests to %d",
                           client->config.outStandingPublishRequests);
        } else {
            UA_LOG_ERROR(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                         "Too many publishrequest when outStandingPublishRequests = 1");
        }
        return;
    }

    if(response->responseHeader.serviceResult == UA_STATUSCODE_BADSHUTDOWN)
        return;

    if(!client->publishNotificationCallback) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADNOSUBSCRIPTION;
        return;
    }

    if(response->responseHeader.serviceResult == UA_STATUSCODE_BADSESSIONCLOSED) {
        if(client->state >= UA_CLIENTSTATE_SESSION) {
            UA_LOG_WARNING(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                           "Received Publish Response with code %s",
                            UA_StatusCode_name(response->responseHeader.serviceResult));
        }
        return;
    }

    if(response->responseHeader.serviceResult == UA_STATUSCODE_BADSESSIONIDINVALID) {
        UA_Client_disconnect(client); /* TODO: This should be handled before the process callback */
        UA_LOG_WARNING(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                       "Received BadSessionIdInvalid");
        return;
    }

    if(response->responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
        UA_LOG_WARNING(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                       "Received Publish Response with code %s",
                       UA_StatusCode_name(response->responseHeader.serviceResult));
        return;
    }

    /* Process the notification messages */
    if(client->publishNotificationCallback)
        for(size_t k = 0; k < msg->notificationDataSize; ++k)
            client->publishNotificationCallback(client, /*response->subscriptionId,*/ &msg->notificationData[k], client->connection.lowOPCUAData);

    /* Add to the list of pending acks */
    for(size_t i = 0; i < response->availableSequenceNumbersSize; i++) {
        if(response->availableSequenceNumbers[i] != msg->sequenceNumber)
            continue;
        UA_Client_NotificationsAckNumber *tmpAck = (UA_Client_NotificationsAckNumber*)
            UA_malloc(sizeof(UA_Client_NotificationsAckNumber));
        if(!tmpAck) {
            UA_LOG_WARNING(&client->config.logger, UA_LOGCATEGORY_CLIENT,
                           "Not enough memory to store the acknowledgement for a publish "
                           "message");
            break;
        }   
        tmpAck->subAck.sequenceNumber = msg->sequenceNumber;
        LIST_INSERT_HEAD(&client->pendingNotificationsAcks, tmpAck, listEntry);
        break;
    } 
}

static void
processPublishResponseAsync(UA_Client *client, void *userdata, UA_UInt32 requestId,
                            void *response) {
    UA_PublishRequest *req = (UA_PublishRequest*)userdata;
    UA_PublishResponse *res = (UA_PublishResponse*)response;

    /* Process the response */
    UA_Client_Subscriptions_processPublishResponse(client, req, res);

    /* Delete the cached request */
    UA_PublishRequest_delete(req);

    /* Fill up the outstanding publish requests */
    UA_Client_Subscriptions_backgroundPublish(client);
}

void
UA_Client_Subscriptions_backgroundPublishInactivityCheck(UA_Client *client) {
    // This function calls a callback which by default does not do anything anyways. Removed
}

UA_StatusCode
UA_Client_Subscriptions_backgroundPublish(UA_Client *client) {
    if(client->state < UA_CLIENTSTATE_SESSION)
        return UA_STATUSCODE_BADSERVERNOTCONNECTED;

    /* The session must have at least one subscription */
    if(!client->publishNotificationCallback)
        return UA_STATUSCODE_GOOD;

    while(client->currentlyOutStandingPublishRequests < client->config.outStandingPublishRequests) {
        UA_PublishRequest *request = UA_PublishRequest_new();
        if (!request)
            return UA_STATUSCODE_BADOUTOFMEMORY;

        request->requestHeader.timeoutHint=60000;
        UA_StatusCode retval = UA_Client_preparePublishRequest(client, request);
        if(retval != UA_STATUSCODE_GOOD) {
            UA_PublishRequest_delete(request);
            return retval;
        }
    
        UA_UInt32 requestId;
        client->currentlyOutStandingPublishRequests++;

        /* Disable the timeout, it is treat in UA_Client_Subscriptions_backgroundPublishInactivityCheck */
        retval = __UA_Client_AsyncServiceEx(client, request, &UA_TYPES[UA_TYPES_PUBLISHREQUEST],
                                            processPublishResponseAsync,
                                            &UA_TYPES[UA_TYPES_PUBLISHRESPONSE],
                                            (void*)request, &requestId, 0, 1);
        if(retval != UA_STATUSCODE_GOOD) {
            UA_PublishRequest_delete(request);
            return retval;
        }
    }

    return UA_STATUSCODE_GOOD;
}

#endif /* UA_ENABLE_SUBSCRIPTIONS */
