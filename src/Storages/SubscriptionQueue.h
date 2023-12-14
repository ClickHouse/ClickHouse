#pragma once

#include <atomic>
#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>

#include <Common/EventFD.h>

#include <Processors/Chunk.h>

namespace DB
{

class Subscriber
{
public:
    void push(Chunk chunk);
    std::list<Chunk> extractAll();

    // returns event_fd's native handle for unix systems
    // otherwise returns nullopt
    std::optional<int> fd() const;

    // cancels waiting for new data
    void cancel();

private:
    // data
    std::mutex mutex;
    std::list<Chunk> ready_chunks;

    // for unix realization
    EventFD new_chunks_event;

    // for other os
    bool cancelled = false;
    std::condition_variable empty_chunks;
};

using SubscriberPtr = std::shared_ptr<Subscriber>;

// Structure for managing the subscriptions,
// necessary for streaming requests to work
class SubscriptionQueue
{
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
