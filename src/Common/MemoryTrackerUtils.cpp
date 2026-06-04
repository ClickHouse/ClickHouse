#include <algorithm>
#include <limits>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
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
    auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker();
    if (!thread_memory_tracker || thread_memory_tracker->level != VariableContext::Thread)
        return 0;

    auto * query_process_memory_tracker = thread_memory_tracker->getParent();
    if (!query_process_memory_tracker || query_process_memory_tracker->level != VariableContext::Process)
        return 0;

    return query_process_memory_tracker->get();
}


extern MemoryTracker total_memory_tracker;

size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread)
{
    if (min_free_per_thread == 0 || max_threads <= 1)
        return max_threads;

    Int64 hard_limit = total_memory_tracker.getHardLimit();
    if (hard_limit <= 0)
        return max_threads;

    Int64 tracked = total_memory_tracker.get();
    Int64 free_memory = hard_limit - tracked;

    if (free_memory <= 0)
        return 1;

    auto allowed = static_cast<size_t>(static_cast<UInt64>(free_memory) / min_free_per_thread);
    if (allowed < 1)
        return 1;
    if (allowed < max_threads)
        return allowed;
    return max_threads;
}
