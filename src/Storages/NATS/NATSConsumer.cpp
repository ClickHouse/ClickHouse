#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/NATSConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include "Poco/Timer.h"
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONNECT_NATS;
}

NATSConsumer::NATSConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    Poco::Logger * log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(connection_)
    , storage(storage_)
    , subjects(subjects_)
    , log(log_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
    , received(queue_size_)
{
}

void NATSConsumer::subscribe()
{
    if (subscribed)
        return;

    for (const auto & subject : subjects)
    {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(
            &ns, connection->getConnection(), subject.c_str(), queue_name.c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            LOG_DEBUG(log, "Subscribed to subject {}", subject);
            natsSubscription_SetPendingLimits(ns, -1, -1);
            subscriptions.emplace_back(ns, &natsSubscription_Destroy);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}", subject);
        }
    }
    subscribed = true;
}

void NATSConsumer::unsubscribe()
{
    for (const auto & subscription : subscriptions)
        natsSubscription_Unsubscribe(subscription.get());
}

ReadBufferPtr NATSConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

void NATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * nats_consumer = static_cast<NATSConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg);

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg), msg_length);
        String subject = natsMsg_GetSubject(msg);

        MessageData data = {
            .message = message_received,
            .subject = subject,
        };
        if (!nats_consumer->received.push(std::move(data)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

        nats_consumer->storage.startStreaming();
    }

    natsMsg_Destroy(msg);
}

/// JetStream

NATSJetStreamConsumer::NATSJetStreamConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    std::vector<String> & subjects_,
    String & stream_,
    String & consumer_,
    Poco::Logger * log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(connection_)
    , storage(storage_)
    , subjects(subjects_)
    , stream(stream_)
    , consumer(consumer_)
    , log(log_)
    , stopped(stopped_)
    , received(queue_size_)
    , js(nullptr, &jsCtx_Destroy)
{
}

void NATSJetStreamConsumer::subscribe()
{
    if (subscribed)
        return;

    jsErrCode jerr = static_cast<jsErrCode>(0);
    jsOptions jsOpts;
    jsSubOptions so;

    int status = 0; 

    if (status == NATS_OK)
    {
        so.Stream = stream.c_str();
        so.Consumer = consumer.c_str();
    }

    if (status == NATS_OK)
        status = jsOptions_Init(&jsOpts);

    if (status == NATS_OK)
        status = jsSubOptions_Init(&so);

    if (status == NATS_OK)
    {
        jsCtx * local_js;
        status = natsConnection_JetStream(&local_js, connection->getConnection(), &jsOpts);
        js = {local_js, &jsCtx_Destroy};
    }

    for (const auto & subject : subjects)
    {
        natsSubscription * ns;
        status = js_Subscribe(&ns, js.get(), subject.c_str(), onMsg, nullptr, &jsOpts, &so, &jerr);
        if (status == NATS_OK)
        {
            LOG_DEBUG(log, "Subscribed to subject {}", subject);
            // natsSubscription_SetPendingLimits(ns, -1, -1);
            subscriptions.emplace_back(ns, &natsSubscription_Destroy);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}", subject);
        }
    }

    subscribed = true;
}

void NATSJetStreamConsumer::unsubscribe()
{
    for (const auto & subscription : subscriptions)
        natsSubscription_Unsubscribe(subscription.get());
}

ReadBufferPtr NATSJetStreamConsumer::consume()
{
    if (stopped || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

void NATSJetStreamConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * nats_consumer = static_cast<NATSJetStreamConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg);

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg), msg_length);
        String subject = natsMsg_GetSubject(msg);

        MessageData data = {
            .message = message_received,
            .subject = subject,
        };
        if (!nats_consumer->received.push(std::move(data)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

        nats_consumer->storage.startStreaming();
    }

    natsMsg_Destroy(msg);
}

}
