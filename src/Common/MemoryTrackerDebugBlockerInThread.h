#pragma once

#include <base/defines.h> /// DEBUG_OR_SANITIZER_BUILD

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <cstdint>

struct MemoryTrackerDebugBlockerInThread
{
public:
    MemoryTrackerDebugBlockerInThread();
    ~MemoryTrackerDebugBlockerInThread();

    static bool isBlocked() { return counter > 0; }
    static constexpr bool isEnabled() { return true; }

private:
    static thread_local uint64_t counter;
};
#else
struct MemoryTrackerDebugBlockerInThread
{
public:
    static constexpr bool isEnabled() { return false; }
    static constexpr bool isBlocked() { return true; }
};
#endif
