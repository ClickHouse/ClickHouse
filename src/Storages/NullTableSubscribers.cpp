#include <Storages/NullTableSubscribers.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_SLOW;
}

NullTableSubscription::NullTableSubscription(size_t max_queued_blocks_)
    : max_queued_blocks(max_queued_blocks_)
{
}

void NullTableSubscription::signalLocked()
{
    if (signaled)
        return;
    signaled = true;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    event.write();
#endif
}

void NullTableSubscription::push(const Block & block)
{
    std::lock_guard lock(mutex);
    if (overflowed)
        return;

    if (queue.size() >= max_queued_blocks)
    {
        /// Drop the queued blocks: the subscriber is going to fail anyway.
        queue.clear();
        overflowed = true;
    }
    else
        queue.push_back(block);

    signalLocked();
}

void NullTableSubscription::wakeUp()
{
    std::lock_guard lock(mutex);
    signalLocked();
}

bool NullTableSubscription::tryPop(Block & block)
{
    std::lock_guard lock(mutex);

    if (signaled)
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        /// Resets the counter of the eventfd. It was written, so it does not block.
        event.read();
#endif
        signaled = false;
    }

    if (overflowed)
        throw Exception(ErrorCodes::TOO_SLOW,
            "The streaming query fell behind the inserts into the Null table by more than {} blocks", max_queued_blocks);

    if (queue.empty())
        return false;

    block = std::move(queue.front());
    queue.pop_front();

    /// Keep the subscription signaled while there are more blocks.
    if (!queue.empty())
        signalLocked();

    return true;
}

NullTableSubscriptionPtr NullTableSubscribers::subscribe(size_t max_queued_blocks)
{
    auto subscription = std::make_shared<NullTableSubscription>(max_queued_blocks);
    std::lock_guard lock(mutex);
    /// Also reclaim the ended subscriptions here, not only on insert: otherwise starting and cancelling readers
    /// of a table without inserts would grow the list without a bound.
    std::erase_if(subscriptions, [](const auto & weak) { return weak.expired(); });
    subscriptions.push_back(subscription);
    num_subscribers.store(subscriptions.size(), std::memory_order_relaxed);
    return subscription;
}

void NullTableSubscribers::push(const Block & block)
{
    std::lock_guard lock(mutex);
    for (auto it = subscriptions.begin(); it != subscriptions.end();)
    {
        if (auto subscription = it->lock())
        {
            subscription->push(block);
            ++it;
        }
        else
            it = subscriptions.erase(it);
    }
    num_subscribers.store(subscriptions.size(), std::memory_order_relaxed);
}

}
