#pragma once

#include <cstdint>
#include <optional>
#include <Common/VariableContext.h>

namespace DB
{
class PageCache;
class TraceCollector;
}

/// To be able to temporarily stop memory tracking from current thread.
struct MemoryTrackerBlockerInThread
{
private:
    static thread_local VariableContext level;

    std::optional<VariableContext> previous_level;

public:
    /// level_ - block in level and above
    explicit MemoryTrackerBlockerInThread(VariableContext level_ = VariableContext::User);

    MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread &&) noexcept;
    MemoryTrackerBlockerInThread & operator=(MemoryTrackerBlockerInThread &&) noexcept;

    void reset();

    ~MemoryTrackerBlockerInThread();

    static bool isBlocked(VariableContext current_level)
    {
        return current_level >= level;
    }

    static bool isBlockedAny()
    {
        return level < VariableContext::Max;
    }

    static VariableContext getLevel()
    {
        return level;
    }
};
