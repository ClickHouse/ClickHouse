#include <Common/MemoryTrackerBlockerInThread.h>
#include <base/defines.h>
#include <utility>

// MemoryTrackerBlockerInThread
constinit FiberLocal<VariableContext, FiberLocalSlot::MEMORY_TRACKER_BLOCKER_LEVEL, /* default_value = */ VariableContext::Max> MemoryTrackerBlockerInThread::level;

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_)
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
