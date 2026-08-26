#include <algorithm>
#include <limits>
#include <Common/CurrentThread.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>

std::optional<UInt64> getMostStrictAvailableSystemMemory()
{
    MemoryTracker * query_memory_tracker = nullptr;
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
    auto * current_memory_tracker = DB::CurrentThread::getMemoryTracker();
    while (current_memory_tracker && current_memory_tracker->level == VariableContext::Thread)
        current_memory_tracker = current_memory_tracker->getParent();

    if (!current_memory_tracker || current_memory_tracker->level != VariableContext::Process)
        return 0;

    return current_memory_tracker->get();
}


void setCurrentQueryMemoryDriftExpected()
{
    /// Every task tracker up to the user, since work nested in a query - a merge started by `OPTIMIZE`, a view -
    /// leaves the memory behind on its own tracker and on the query's.
    for (auto * tracker = DB::CurrentThread::getMemoryTracker(); tracker; tracker = tracker->getParent())
    {
        if (tracker->level == VariableContext::Process)
            tracker->setDriftExpected();
    }
}


std::unique_ptr<MemoryTracker> createTrackerForMemoryOutlivingCurrentQuery()
{
    auto * query_memory_tracker = DB::CurrentThread::getMemoryTracker();
    while (query_memory_tracker && query_memory_tracker->level == VariableContext::Thread)
        query_memory_tracker = query_memory_tracker->getParent();

    if (!query_memory_tracker || query_memory_tracker->level != VariableContext::Process)
        return nullptr;

    auto * user_memory_tracker = query_memory_tracker->getParent();
    while (user_memory_tracker && user_memory_tracker->level != VariableContext::User)
        user_memory_tracker = user_memory_tracker->getParent();

    if (!user_memory_tracker)
        return nullptr;

    auto tracker = std::make_unique<MemoryTracker>(
        user_memory_tracker, VariableContext::Process, /*log_peak_memory_usage_in_destructor*/ false);

    /// Settling against the user on destruction is the point of this tracker, not a leak to report.
    tracker->setDriftExpected();

    return tracker;
}

void giveMemoryBackToCurrentQuery(MemoryTracker & tracker)
{
    Int64 size = tracker.get();
    if (size <= 0)
        return;

    auto * query_memory_tracker = DB::CurrentThread::getMemoryTracker();
    while (query_memory_tracker && query_memory_tracker->level == VariableContext::Thread)
        query_memory_tracker = query_memory_tracker->getParent();

    if (!query_memory_tracker || query_memory_tracker->level != VariableContext::Process)
        return;

    /// The user is charged either way; this only moves who holds the bytes below them.
    query_memory_tracker->transferUpTo(VariableContext::User, -size);
    tracker.transferUpTo(VariableContext::User, size);
}


std::unique_ptr<MemoryTracker> tryCreateMemoryTrackerUnderCurrentQuery()
{
    auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker();
    if (!thread_memory_tracker || thread_memory_tracker->level != VariableContext::Thread)
        return nullptr;

    auto * query_memory_tracker = thread_memory_tracker->getParent();
    if (!query_memory_tracker || query_memory_tracker->level != VariableContext::Process)
        return nullptr;

    return std::make_unique<MemoryTracker>(query_memory_tracker, VariableContext::Thread);
}


extern MemoryTracker total_memory_tracker;

static size_t getMaxThreadsForAvailableMemoryImpl(size_t max_threads, UInt64 min_free_per_thread)
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
size_t getMaxThreadsForAvailableMemory(size_t max_threads, UInt64 min_free_per_thread)
{
    size_t effective_threads = getMaxThreadsForAvailableMemoryImpl(max_threads, min_free_per_thread);
    if (effective_threads != max_threads)
        LOG_DEBUG(getLogger("MemoryTrackerUtils"), "Lower number of threads for query to {} ({} requested)", effective_threads, max_threads);
    return effective_threads;
}
