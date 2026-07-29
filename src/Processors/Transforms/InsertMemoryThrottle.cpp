#include <Processors/Transforms/InsertMemoryThrottle.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event InsertPipelineThrottled;
}


namespace DB
{

MemorySnapshot QueryMemoryProvider::get() const
{
    MemorySnapshot result;

    if (!tracker)
    {
        if (auto * current_tracker = CurrentThread::getMemoryTracker())
        {
            while (current_tracker->getHardLimit() <= 0)
            {
                auto * parent = current_tracker->getParent();
                if (!parent)
                    break;
                current_tracker = parent;
            }
            result.used_bytes = current_tracker->get();
            result.hard_limit_bytes = current_tracker->getHardLimit();
        }
    }
    else
    {
        result.used_bytes = tracker->get();
        result.hard_limit_bytes = tracker->getHardLimit();
    }

    return result;
}


bool InsertMemoryThrottle::isThrottled()
{
    if (!settings.enabled)
        return false;

    MemorySnapshot mem = mem_provider->get();

    if (mem.hard_limit_bytes <= 0)
        return false;

    const double ratio = static_cast<double>(mem.used_bytes) / static_cast<double>(mem.hard_limit_bytes);

    const bool was_throttled = throttled.load(std::memory_order_relaxed);

    /// once throttled, stay throttled until we drop below low_threshold
    const bool now_throttled = was_throttled ? ratio >= settings.low_threshold : ratio >= settings.high_threshold;

    if (was_throttled != now_throttled)
    {
        throttled.store(now_throttled, std::memory_order_relaxed);

        if (now_throttled)
            ProfileEvents::increment(ProfileEvents::InsertPipelineThrottled);
    }

    return now_throttled;
}

InsertMemoryThrottle::Snapshot InsertMemoryThrottle::getSnapshot() const
{
    Snapshot snap;
    snap.memory = mem_provider->get();
    snap.throttled = throttled.load(std::memory_order_relaxed);
    return snap;
}

}
