#include <Common/MemoryTrackerBlockerInThread.h>
#include <base/defines.h>
#include <cstdlib>
#include <utility>

// MemoryTrackerBlockerInThread
thread_local VariableContext MemoryTrackerBlockerInThread::level = VariableContext::Max;

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_)
{
    /// Prevent unblocking a higher-level tracker
    if (level <= level_)
        return;

    previous_level = level;
    level = level_;
}

MemoryTrackerBlockerInThread::~MemoryTrackerBlockerInThread()
{
    reset();
}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread && rhs) noexcept
    : previous_level(std::exchange(rhs.previous_level, std::nullopt))
{}

MemoryTrackerBlockerInThread & MemoryTrackerBlockerInThread::operator=(MemoryTrackerBlockerInThread && rhs) noexcept
{
    reset();
    previous_level = std::exchange(rhs.previous_level, std::nullopt);
    return *this;
}

void MemoryTrackerBlockerInThread::reset()
{
    if (previous_level.has_value())
    {
        level = previous_level.value();
        previous_level.reset();

        /// Having MemoryTracker blocked on Global level upon exit is a bug.
        if (level == VariableContext::Global) [[unlikely]]
            abort();
    }
}
