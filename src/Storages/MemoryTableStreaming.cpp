#include <Storages/MemoryTableStreaming.h>

namespace DB
{

void MemoryTableWakeup::notify()
{
    std::lock_guard lock(mutex);
    if (signaled)
        return;
    signaled = true;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    event.write();
#endif
}

void MemoryTableWakeup::reset()
{
    std::lock_guard lock(mutex);
    if (!signaled)
        return;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// Resets the counter of the eventfd. It was written, so it does not block.
    event.read();
#endif
    signaled = false;
}

MemoryTableWakeupPtr MemoryTableSubscribers::subscribe()
{
    auto wakeup = std::make_shared<MemoryTableWakeup>();
    std::lock_guard lock(mutex);
    /// The subscriptions of the finished queries are removed here as well as in `notify`, so that they do not
    /// accumulate in a table that is read (e.g. by an incremental refresh) but not written.
    std::erase_if(wakeups, [](const auto & weak) { return weak.expired(); });
    wakeups.push_back(wakeup);
    num_subscribers.store(wakeups.size(), std::memory_order_relaxed);
    return wakeup;
}

void MemoryTableSubscribers::notify()
{
    if (num_subscribers.load(std::memory_order_relaxed) == 0)
        return;

    std::lock_guard lock(mutex);
    for (auto it = wakeups.begin(); it != wakeups.end();)
    {
        if (auto wakeup = it->lock())
        {
            wakeup->notify();
            ++it;
        }
        else
            it = wakeups.erase(it);
    }
    num_subscribers.store(wakeups.size(), std::memory_order_relaxed);
}

}
