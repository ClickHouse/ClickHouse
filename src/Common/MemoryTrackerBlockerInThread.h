#pragma once

#include <cstdint>
#include <optional>
#include <Common/VariableContext.h>

namespace DB
{
    class PageCache;
}

/// To be able to temporarily stop memory tracking from current thread.
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
    MemoryTrackerBlockerInThread(std::nullopt_t);
    MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread &&);
    MemoryTrackerBlockerInThread & operator=(MemoryTrackerBlockerInThread &&);
    void reset();

public:
    explicit MemoryTrackerBlockerInThread();
    ~MemoryTrackerBlockerInThread();

    static bool isBlocked(VariableContext current_level)
    {
        return counter > 0 && current_level >= level;
    }

    friend class MemoryTracker;
    friend struct AllocationTrace;
    friend class DB::PageCache;
};
