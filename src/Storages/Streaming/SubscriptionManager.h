#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/SharedMutex.h>

#include <shared_mutex>
#include <list>
#include <vector>

namespace DB
{

/// Utility for storing and managing multiple subscriptions.
class StreamSubscriptionManager
{
    void clean();

    std::shared_lock<SharedMutex> lockShared() const;
    std::unique_lock<SharedMutex> lockExclusive() const;

public:
    /// Adds subscription for manager, not transfers lifetime
    void registerSubscription(StreamSubscriptionPtr subscription);

    /// Returns all registered subscriptions
    std::vector<StreamSubscriptionPtr> takeAllSubscriptions();

    bool isEmpty() const;
    bool hasSome() const;

private:
    /// Lock required for all changes with subscriptions
    mutable SharedMutex rwlock;

    /// List of all subscriptions
    std::list<StreamSubscriptionWeakPtr> subscriptions;
};

}
