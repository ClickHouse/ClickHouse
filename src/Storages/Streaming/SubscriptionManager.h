#pragma once

#include <Storages/Streaming/IStreamSubscription.h>

#include <Common/SharedMutex.h>

#include <shared_mutex>
#include <list>
#include <vector>

namespace DB
{

/// Utility for storing and managing multiple subscriptions.
/// Registration only: holds weak pointers to subscriptions created elsewhere,
/// and exposes `collectSubscriptions` for fan-out on events (e.g. commit
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

    /// Returns strong pointers to every live subscription, keeping them alive for the caller.
    ///
    /// A caller that serves the returned subscriptions from some shared state (e.g. a snapshot of
    /// the visible parts) must collect the subscriptions *first* and read that state only
    /// afterwards. Everything in the returned list was registered before this call, so a state read
    /// after it is at least as fresh as every subscription in the list. Reading the state first
    /// would let a subscription registered in between be served from a snapshot that predates it.
    std::vector<StreamSubscriptionPtr> collectSubscriptions();

    /// returns true if no active subscriptions are registered in manager
    bool isEmpty() const;

    /// opposite to isEmpty
    bool hasSome() const;

private:
    /// Lock required for all changes with subscriptions:
    /// - Add new subscription
    /// - Collect subscriptions
    mutable SharedMutex rwlock;

    /// List of all subscriptions
    std::list<StreamSubscriptionWeakPtr> subscriptions;
};

}
