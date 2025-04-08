#include <Storages/NATS/NATSCoreConsumer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int INVALID_STATE;
}

NATSCoreConsumer::NATSCoreConsumer(
    NATSConnectionPtr connection,
    const std::vector<String> & subjects,
    const String & subscribe_queue_name,
    LoggerPtr log,
    uint32_t queue_size,
    const std::atomic<bool> & stopped)
    : INATSConsumer(std::move(connection), subjects, subscribe_queue_name, log, queue_size, stopped)
{
}

void NATSCoreConsumer::subscribe()
{
    if (isSubscribed())
        return;

    std::vector<NATSSubscriptionPtr> created_subscriptions;
    for (const auto & subject : getSubjects())
    {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(&ns, getNativeConnection(), subject.c_str(), getQueueName().c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            created_subscriptions.emplace_back(ns, &natsSubscription_Destroy);
            LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);

            natsSubscription_SetPendingLimits(ns, -1, -1);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe consumer {} to subject {}", static_cast<void*>(this), subject);
        }
    }
    LOG_DEBUG(getLogger(), "Consumer {} subscribed to {} subjects", static_cast<void*>(this), created_subscriptions.size());

    setSubscriptions(std::move(created_subscriptions));
}

}
