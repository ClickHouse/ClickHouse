#include <algorithm>
#include <limits>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

size_t getMaxBytesBeforeExternalProcessing(size_t max_bytes, double max_bytes_ratio, std::string_view ratio_setting_name)
{
    std::optional<size_t> threshold;
    if (max_bytes != 0)
        threshold = max_bytes;

    if (max_bytes_ratio != 0.)
    {
        if (max_bytes_ratio < 0 || max_bytes_ratio >= 1.)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS, "Setting {} should be >= 0 and < 1 ({})", ratio_setting_name, max_bytes_ratio);

        auto available_system_memory = getMostStrictAvailableSystemMemory();
        if (available_system_memory.has_value())
        {
            /// Zero disables spilling, so an enabled ratio must produce at least a one-byte threshold.
            const size_t ratio_in_bytes
                = std::max<size_t>(1, static_cast<size_t>(static_cast<double>(*available_system_memory) * max_bytes_ratio));
            threshold = threshold ? std::min(*threshold, ratio_in_bytes) : ratio_in_bytes;

            LOG_TRACE(
                getLogger("MemoryTrackerUtils"),
                "Adjusting spill threshold with {} ({}: {}, available system memory: {})",
                formatReadableSizeWithBinarySuffix(ratio_in_bytes),
                ratio_setting_name,
                max_bytes_ratio,
                formatReadableSizeWithBinarySuffix(*available_system_memory));
        }
        else
        {
            LOG_TRACE(getLogger("MemoryTrackerUtils"), "No system memory limits configured. Ignoring {}", ratio_setting_name);
        }
    }

    return threshold.value_or(0);
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
