#pragma once

#include <optional>
#include <Common/FiberLocal.h>
#include <Common/VariableContext.h>

namespace DB
{
class PageCache;
class TraceCollector;
}

/// Temporarily stop memory tracking for the current thread at a level and above.
///
/// For memory that belongs to the server and not to the query, use `ServerOwnedMemoryScope` or `makeServerOwned`
/// from `Common/ServerOwnedMemory.h`. To only avoid `MEMORY_LIMIT_EXCEEDED`, use `LockMemoryExceptionInThread`.
/// Use this directly at `VariableContext::Global`, for the internals of the tracker and of logging.
struct MemoryTrackerBlockerInThread
{
private:
    static constinit FiberLocal<VariableContext, FiberLocalSlot::MEMORY_TRACKER_BLOCKER_LEVEL, /* default_value = */ VariableContext::Max> level;

    std::optional<VariableContext> previous_level;

public:
    /// level_ - block in level and above. A blocker nested in a stronger one keeps the stronger level.
    explicit MemoryTrackerBlockerInThread(VariableContext level_ = VariableContext::User);

    /// Sets exactly `level_`, also when it is weaker than the current one: to flush memory collected under that level.
    struct ExactLevel {};
    MemoryTrackerBlockerInThread(VariableContext level_, ExactLevel);

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
