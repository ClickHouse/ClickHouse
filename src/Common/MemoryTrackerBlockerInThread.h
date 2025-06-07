#pragma once

#include <cstdint>
#include <optional>
#include <Common/VariableContext.h>

namespace DB
{
class PageCache;
class TraceCollector;
}

/// Temporarily stop memory tracking for the current thread.
///
/// Note, that this is more powerful way for blocking memory tracker,
/// use this if you need to avoid accounting some memory for the user queries
/// (i.e. the query initialize some cache)
///
/// In other cases (i.e. you need to just ignore MEMORY_LIMIT_EXCEEDED error)
/// prefer LockMemoryExceptionInThread.
struct MemoryTrackerBlockerInThread
{
private:
    static thread_local uint64_t counter;
    static thread_local VariableContext level;

    VariableContext previous_level;
    uint64_t previous_counter; // if UINT64_MAX, this blocker is empty

    /// level_ - block in level and above
    explicit MemoryTrackerBlockerInThread(VariableContext level_);

    /// Empty.
    explicit MemoryTrackerBlockerInThread(std::nullopt_t);
    MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread &&) noexcept;
    MemoryTrackerBlockerInThread & operator=(MemoryTrackerBlockerInThread &&) noexcept;
    void reset();

public:
    explicit MemoryTrackerBlockerInThread();
    ~MemoryTrackerBlockerInThread();

    static bool isBlocked(VariableContext current_level)
    {
        return counter > 0 && current_level >= level;
    }

    static bool isBlockedAny()
    {
        return counter > 0;
    }

    friend class MemoryTracker;
    friend struct AllocationTrace;
    friend class DB::PageCache;
    friend class DB::TraceCollector;
};
