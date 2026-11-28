#include <Storages/NATS/NATSCoreConsumer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

void NATSCoreConsumer::subscribe()
{
    if (isSubscribed())
        return;

    std::vector<NATSSubscriptionPtr> created_subscriptions;
    created_subscriptions.reserve(getSubjects().size());

    for (const auto & subject : getSubjects())
    {
        natsSubscription * subscription;
        auto status = natsConnection_QueueSubscribe(&subscription, getNativeConnection(), subject.c_str(), getQueueName().c_str(), onMsg, static_cast<void *>(this));
        if (status != NATS_OK)
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe consumer {} to subject {}", static_cast<void*>(this), subject);

        created_subscriptions.emplace_back(subscription, &natsSubscription_Destroy);
        LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);

        natsSubscription_SetPendingLimits(subscription, -1, -1);
    }
    LOG_DEBUG(getLogger(), "Consumer {} subscribed to {} subjects", static_cast<void*>(this), created_subscriptions.size());

    setSubscriptions(std::move(created_subscriptions));
}

}
