#include <Common/MemoryTrackerBlockerInThread.h>
#include <base/defines.h>
#include <algorithm>
#include <utility>

// MemoryTrackerBlockerInThread
constinit FiberLocal<VariableContext, FiberLocalSlot::MEMORY_TRACKER_BLOCKER_LEVEL, /* default_value = */ VariableContext::Max> MemoryTrackerBlockerInThread::level;

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_)
    : previous_level(level)
{
    /// A blocker nested in a stronger one must not weaken it.
    level = std::min(level.get(), level_);
}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_, ExactLevel)
    : previous_level(level)
{
    level = level_;
}

MemoryTrackerBlockerInThread::~MemoryTrackerBlockerInThread()
{
    reset();
}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread && rhs) noexcept
    : previous_level(std::exchange(rhs.previous_level, std::nullopt)) {}

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
    }
}
