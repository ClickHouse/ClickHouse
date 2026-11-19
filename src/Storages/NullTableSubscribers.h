#pragma once

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Common/EventFD.h>

#include <atomic>
#include <deque>
#include <list>
#include <memory>
#include <mutex>


namespace DB
{

/// A subscription of a streaming query (`SELECT ... FROM null_table STREAM`) to the blocks inserted into a `Null` table.
/// The inserting threads push the blocks; the source of the query pops them. The queue of a subscription is bounded:
/// a subscriber that falls behind by more than `max_queued_blocks` blocks is failed, so that it never blocks the inserts.
class NullTableSubscription
{
public:
    explicit NullTableSubscription(size_t max_queued_blocks_);

    /// Called by an inserting thread.
    void push(const Block & block);

    /// Called by the source. Returns false if there is no block now: then wait until `fd` is readable.
    /// Throws if the subscriber fell behind.
    bool tryPop(Block & block);

    /// Wake up the source, e.g. on cancellation of the query.
    void wakeUp();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    [[nodiscard]] int fd() const { return event.fd; }
#endif

private:
    void signalLocked();

    const size_t max_queued_blocks;

    std::mutex mutex;
    std::deque<Block> queue;
    bool overflowed = false;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// Written once when the subscription becomes signaled, read once when it is reset, so the reads never block.
    EventFD event;
#endif
    bool signaled = false;
};

using NullTableSubscriptionPtr = std::shared_ptr<NullTableSubscription>;

/// The subscriptions to a `Null` table. A subscription ends when its source is destroyed.
class NullTableSubscribers
{
public:
    NullTableSubscriptionPtr subscribe(size_t max_queued_blocks);

    /// Push the block to every subscriber.
    void push(const Block & block);

    bool hasSubscribers() const { return num_subscribers.load(std::memory_order_relaxed) > 0; }

private:
    std::mutex mutex;
    std::list<std::weak_ptr<NullTableSubscription>> subscriptions;
    std::atomic<size_t> num_subscribers{0};
};

}
