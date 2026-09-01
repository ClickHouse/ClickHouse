#include <Common/MemoryTrackerUntrackedAllocationsBlockerInThread.h>
#include <cstdint>

static thread_local constinit uint64_t MemoryTrackerUntrackedAllocationsBlockerInThreadCounter = 0;

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
