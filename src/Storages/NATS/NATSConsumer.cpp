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
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

NATSConsumer::NATSConsumer(
    NATSConnectionPtr connection_,
    std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(std::move(connection_))
    , subjects(subjects_)
    , log(log_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
    , received(queue_size_)
{
}

bool NATSConsumer::isSubscribed() const
{
    return !subscriptions.empty();
}
void NATSConsumer::subscribe()
{
    if (isSubscribed())
        return;

    std::vector<NATSSubscriptionPtr> created_subscriptions;
    for (const auto & subject : subjects)
    {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(
            &ns, connection->getConnection(), subject.c_str(), queue_name.c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            created_subscriptions.emplace_back(ns, &natsSubscription_Destroy);
            LOG_DEBUG(log, "Subscribed to subject {}", subject);

            natsSubscription_SetPendingLimits(ns, -1, -1);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe consumer {} to subject {}", static_cast<void*>(this), subject);
        }
    }
    LOG_DEBUG(log, "Consumer {} subscribed to {} subjects", static_cast<void*>(this), created_subscriptions.size());

    subscriptions = std::move(created_subscriptions);
}

void NATSConsumer::unsubscribe()
{
    subscriptions.clear();

    LOG_DEBUG(log, "Consumer {} unsubscribed", static_cast<void*>(this));
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
            throw Exception(ErrorCodes::INVALID_STATE, "Could not push to received queue");
    }

    natsMsg_Destroy(msg);
}

}
