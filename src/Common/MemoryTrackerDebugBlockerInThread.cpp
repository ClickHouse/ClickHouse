#include <Common/MemoryTrackerDebugBlockerInThread.h>

#ifdef DEBUG_OR_SANITIZER_BUILD

thread_local uint64_t MemoryTrackerDebugBlockerInThread::counter = 0;

MemoryTrackerDebugBlockerInThread::MemoryTrackerDebugBlockerInThread()
{
    ++counter;
}

MemoryTrackerDebugBlockerInThread::~MemoryTrackerDebugBlockerInThread()
{
    --counter;
}
#endif
