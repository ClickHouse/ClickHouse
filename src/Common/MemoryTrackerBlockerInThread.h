#pragma once

#include <cstdint>
#include <Common/VariableContext.h>

/// To be able to temporarily stop memory tracking from current thread.
struct MemoryTrackerBlockerInThread
{
private:
    static thread_local uint64_t counter;
    static thread_local VariableContext level;

    VariableContext previous_level;
public:
    /// level_ - block in level and above
    explicit MemoryTrackerBlockerInThread(VariableContext level_ = VariableContext::User);
    ~MemoryTrackerBlockerInThread();

    MemoryTrackerBlockerInThread(const MemoryTrackerBlockerInThread &) = delete;
    MemoryTrackerBlockerInThread & operator=(const MemoryTrackerBlockerInThread &) = delete;

    static bool isBlocked(VariableContext current_level)
    {
        return counter > 0 && current_level >= level;
    }
};
