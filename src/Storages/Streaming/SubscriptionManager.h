#pragma once

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
    void pushToAll(Block block);

private:
    // Lock required for all changes with subscriptions:
    // - Add new listener
    // - Push chunk
    mutable std::shared_mutex rwlock;

    // List of all subscriptions
    std::list<std::weak_ptr<StreamSubscription>> subscriptions;
};

}
