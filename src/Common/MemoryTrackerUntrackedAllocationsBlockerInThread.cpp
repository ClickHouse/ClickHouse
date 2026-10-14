#include <Common/MemoryTrackerUntrackedAllocationsBlockerInThread.h>
#include <Common/FiberLocal.h>
#include <cstdint>

static constinit FiberLocal<uint64_t, FiberLocalSlot::MEMORY_TRACKER_UNTRACKED_ALLOCATIONS_BLOCKER_COUNTER> MemoryTrackerUntrackedAllocationsBlockerInThreadCounter;

MemoryTrackerUntrackedAllocationsBlockerInThread::MemoryTrackerUntrackedAllocationsBlockerInThread()
{
    MemoryTrackerUntrackedAllocationsBlockerInThreadCounter = MemoryTrackerUntrackedAllocationsBlockerInThreadCounter + 1;
}

MemoryTrackerUntrackedAllocationsBlockerInThread::~MemoryTrackerUntrackedAllocationsBlockerInThread()
{
    MemoryTrackerUntrackedAllocationsBlockerInThreadCounter = MemoryTrackerUntrackedAllocationsBlockerInThreadCounter - 1;
}

bool MemoryTrackerUntrackedAllocationsBlockerInThread::isBlocked()
{
    return MemoryTrackerUntrackedAllocationsBlockerInThreadCounter > 0;
}
