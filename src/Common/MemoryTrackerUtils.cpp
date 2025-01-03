#include <algorithm>
#include <limits>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerUtils.h>

std::optional<UInt64> getMostStrictAvailableSystemMemory()
{
    MemoryTracker * query_memory_tracker;
    if (query_memory_tracker = DB::CurrentThread::getMemoryTracker(); !query_memory_tracker)
        return {};
    /// query-level memory tracker
    if (query_memory_tracker = query_memory_tracker->getParent(); !query_memory_tracker)
        return {};

    Int64 available = std::numeric_limits<Int64>::max();
    MemoryTracker * system_memory_tracker = query_memory_tracker->getParent();
    while (system_memory_tracker)
    {
        if (Int64 tracker_limit = system_memory_tracker->getHardLimit(); tracker_limit > 0)
        {
            Int64 tracker_used = system_memory_tracker->get();
            Int64 tracker_available = std::clamp<Int64>(tracker_limit - tracker_used, 0, std::numeric_limits<Int64>::max());
            available = std::min(available, tracker_available);
        }
        system_memory_tracker = system_memory_tracker->getParent();
    }
    if (available == std::numeric_limits<Int64>::max())
        return {};
    return available;
}

std::optional<UInt64> getCurrentQueryHardLimit()
{
    Int64 hard_limit = std::numeric_limits<Int64>::max();
    MemoryTracker * memory_tracker = DB::CurrentThread::getMemoryTracker();
    while (memory_tracker)
    {
        if (Int64 tracker_limit = memory_tracker->getHardLimit(); tracker_limit > 0)
        {
            hard_limit = std::min(hard_limit, tracker_limit);
        }
        memory_tracker = memory_tracker->getParent();
    }
    if (hard_limit == std::numeric_limits<Int64>::max())
        return {};
    return hard_limit;
}


Int64 getCurrentQueryMemoryUsage()
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = DB::CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            return memory_tracker->get();
    return 0;
}
