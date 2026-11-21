#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/SharedMutex.h>

#include <shared_mutex>
#include <functional>
#include <list>

namespace DB
{

/// Utility for storing and managing multiple subscriptions.
/// Registration only: holds weak pointers to subscriptions created elsewhere,
/// and exposes `executeOnEachSubscription` for fan-out on events (e.g. commit
/// notifications). Expired subscriptions are cleaned up lazily.
class StreamSubscriptionManager
{
    void clean();

    /// returns locked mutex
    std::shared_lock<SharedMutex> lockShared() const;
    std::unique_lock<SharedMutex> lockExclusive() const;

public:
    /// adds subscription for manager, not transfers lifetime
    void registerSubscription(StreamSubscriptionPtr subscription);

    /// runs function on every subscription
    void executeOnEachSubscription(const std::function<void(StreamSubscriptionPtr & subscription)> & func);

    /// returns true if no active subscriptions are registered in manager
    bool isEmpty() const;

    /// opposite to isEmpty
    bool hasSome() const;

private:
    /// Lock required for all changes with subscriptions:
    /// - Add new subscription
    /// - Execute function
    mutable SharedMutex rwlock;

    /// List of all subscriptions
    std::list<StreamSubscriptionWeakPtr> subscriptions;
};

}
