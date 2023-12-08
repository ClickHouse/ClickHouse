#pragma once

#include <deque>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include <Common/EventFD.h>

#include <Processors/Chunk.h>

namespace DB
{

class Subscriber {
public:
    void push(Chunk chunk);

    std::optional<Chunk> extract();

private:
    EventFD event_fd;

    // data
    std::mutex mutex;
    std::deque<Chunk> ready_chunks;
};

using SubscriberPtr = std::shared_ptr<Subscriber>;

// Structure for managing the subscriptions,
// necessary for streaming requests to work
class SubscriptionQueue {
    void clean();

    // returns locked mutex
    std::shared_lock<std::shared_mutex> lockShared() const;
    std::unique_lock<std::shared_mutex> lockExclusive() const;

public:
    // created new subscription
    SubscriberPtr subscribe();

    // adds new chunk to all subscribers and notifies
    void pushChunk(Chunk chunk);

private:
    // Lock required for all changes with subscriptions:
    // - Add new listener
    // - Push chunk
    mutable std::shared_mutex rwlock;

    // List of all subscriptions
    std::list<std::weak_ptr<Subscriber>> subscribers;
};

}
