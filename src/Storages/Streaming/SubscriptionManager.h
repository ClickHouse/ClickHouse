#pragma once

#include <atomic>
#include <limits>
#include <list>
#include <mutex>
#include <shared_mutex>

#include <Common/EventFD.h>

#include <Core/Block.h>

#include <Storages/Streaming/Subscription_fwd.h>

namespace DB
{

// Utility for propagating data from a single producer to multiple consumers.
// Used for pushing newly inserted data from a table to streaming queries reading from it.
class StreamSubscriptionManager
{
    void clean();

    // returns locked mutex
    std::shared_lock<std::shared_mutex> lockShared() const;
    std::unique_lock<std::shared_mutex> lockExclusive() const;

public:
    // create new subscription
    StreamSubscriptionPtr subscribe();

    // push new chunk to all subscriptions
    void pushToAll(Block block, uint64_t snapshot = std::numeric_limits<uint64_t>::max());

    // returns count of currently running subcriptions
    uint64_t getSubscriptionsCount() const;

    // returns snapshot of manager state
    // returned value can be used in pushToAll
    uint64_t getSnapshot() const;

private:
    // Lock required for all changes with subscriptions:
    // - Add new listener
    // - Push chunk
    mutable std::shared_mutex rwlock;

    // List of all subscriptions
    std::list<std::weak_ptr<StreamSubscription>> subscriptions;

    // Monitors currently alive subscriptions count, to have fast empty check
    std::atomic<uint64_t> subscriptions_count;

    // Sequential number over all subscriptions, that allows to take a snapshot of the state
    std::atomic<uint64_t> subscription_id_counter;
};

}
