#include <Common/MemoryTrackerUntrackedAllocationsBlockerInThread.h>
#include <cstdint>

static thread_local uint64_t MemoryTrackerUntrackedAllocationsBlockerInThreadCounter;

MemoryTrackerUntrackedAllocationsBlockerInThread::MemoryTrackerUntrackedAllocationsBlockerInThread()
{
    ++MemoryTrackerUntrackedAllocationsBlockerInThreadCounter;
}

MemoryTrackerUntrackedAllocationsBlockerInThread::~MemoryTrackerUntrackedAllocationsBlockerInThread()
{
    --MemoryTrackerUntrackedAllocationsBlockerInThreadCounter;
}

bool MemoryTrackerUntrackedAllocationsBlockerInThread::isBlocked()
{
    return MemoryTrackerUntrackedAllocationsBlockerInThreadCounter > 0;
}
