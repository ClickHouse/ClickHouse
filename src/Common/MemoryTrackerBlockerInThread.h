#pragma once

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

    static VariableContext getLevel()
    {
        return level;
    }
};
