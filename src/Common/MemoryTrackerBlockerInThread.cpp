#include <Common/MemoryTrackerBlockerInThread.h>
#include <base/defines.h>
#include <utility>

// MemoryTrackerBlockerInThread
thread_local uint64_t MemoryTrackerBlockerInThread::counter = 0;
thread_local VariableContext MemoryTrackerBlockerInThread::level = VariableContext::Global;

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(VariableContext level_)
    : previous_level(level), previous_counter(counter)
{
    ++counter;
    level = level_;
}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread() : MemoryTrackerBlockerInThread(VariableContext::User)
{
}

MemoryTrackerBlockerInThread::~MemoryTrackerBlockerInThread()
{
    reset();
}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(std::nullopt_t)
    : previous_level(VariableContext::User), previous_counter(UINT64_MAX) {}

MemoryTrackerBlockerInThread::MemoryTrackerBlockerInThread(MemoryTrackerBlockerInThread && rhs) noexcept
    : previous_level(rhs.previous_level), previous_counter(std::exchange(rhs.previous_counter, UINT64_MAX)) {}

MemoryTrackerBlockerInThread & MemoryTrackerBlockerInThread::operator=(MemoryTrackerBlockerInThread && rhs) noexcept
{
    reset();
    previous_level = rhs.previous_level;
    previous_counter = std::exchange(rhs.previous_counter, UINT64_MAX);
    return *this;
}

void MemoryTrackerBlockerInThread::reset()
{
    if (previous_counter != UINT64_MAX)
    {
        --counter;
        level = previous_level;
        chassert(counter == previous_counter);
        previous_counter = UINT64_MAX;
    }
}
