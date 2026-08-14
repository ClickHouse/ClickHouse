#include <Common/MemoryTrackerDebugBlockerInThread.h>

#ifdef DEBUG_OR_SANITIZER_BUILD

#include <cstdint>

static thread_local uint64_t MemoryTrackerDebugBlockerInThreadCounter;

MemoryTrackerDebugBlockerInThread::MemoryTrackerDebugBlockerInThread()
{
    ++MemoryTrackerDebugBlockerInThreadCounter;
}

MemoryTrackerDebugBlockerInThread::~MemoryTrackerDebugBlockerInThread()
{
    --MemoryTrackerDebugBlockerInThreadCounter;
}

bool MemoryTrackerDebugBlockerInThread::isBlocked()
{
    return MemoryTrackerDebugBlockerInThreadCounter > 0;
}
#endif
