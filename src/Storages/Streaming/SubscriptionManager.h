#pragma once

#include <list>
#include <mutex>
#include <shared_mutex>

#include <Common/EventFD.h>

#include <Processors/Chunk.h>

#include <Storages/Streaming/Subscription_fwd.h>

namespace DB
{

// Structure for managing the subscriptions,
// necessary for streaming requests to work
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
    void pushChunk(Chunk chunk);

private:
    // Lock required for all changes with subscriptions:
    // - Add new listener
    // - Push chunk
    mutable std::shared_mutex rwlock;

    // List of all subscriptions
    std::list<std::weak_ptr<StreamSubscription>> subscriptions;
};

}
