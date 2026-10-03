#pragma once

#include <base/defines.h> /// DEBUG_OR_SANITIZER_BUILD

/// Use it if you need to suppress MemoryAllocatedWithoutCheck for known big allocations.
#ifdef DEBUG_OR_SANITIZER_BUILD
struct MemoryTrackerDebugBlockerInThread
{
public:
    MemoryTrackerDebugBlockerInThread();
    ~MemoryTrackerDebugBlockerInThread();

    static bool isBlocked();
};
#else
struct MemoryTrackerDebugBlockerInThread
{
public:
    static constexpr bool isBlocked() { return true; }
};
#endif
