#pragma once

/// Use it if you need to suppress MemoryAllocatedWithoutCheck for known big allocations.
struct MemoryTrackerUntrackedAllocationsBlockerInThread
{
public:
    MemoryTrackerUntrackedAllocationsBlockerInThread();
    ~MemoryTrackerUntrackedAllocationsBlockerInThread();

    static bool isBlocked();
};
