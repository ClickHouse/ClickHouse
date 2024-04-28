#pragma once

#include <atomic>
#include <list>
#include <shared_mutex>

#include <Storages/Streaming/IStreamSubscription.h>

namespace DB
{

/// Utility for storing and managing multiple subscriptions
class StreamSubscriptionManager
{
    void clean();

    /// returns locked mutex
    std::shared_lock<std::shared_mutex> lockShared() const;
    std::unique_lock<std::shared_mutex> lockExclusive() const;

public:
    /// adds subscription for manager, not transfers lifetime
    void registerSubscription(StreamSubscriptionPtr subscription);

    /// runs function on every subscription
    void executeOnEachSubscription(const std::function<void(StreamSubscriptionPtr & subscription)> & func);

    /// returns count of currently running subcriptions
    uint64_t getSubscriptionsCount() const;
    bool isEmpty() const;
    bool hasSome() const;

private:
    /// Lock required for all changes with subscriptions:
    /// - Add new subscription
    /// - Execute function
    mutable std::shared_mutex rwlock;

    /// List of all subscriptions
    std::list<StreamSubscriptionWeakPtr> subscriptions;

    /// Monitors currently alive subscriptions count, to have fast empty check
    std::atomic<uint64_t> subscriptions_count;
};

}
