#pragma once

#include <cstdint>
#include <base/defines.h>

#if defined(OS_LINUX)
#include <sched.h>
#endif

namespace PerCPU
{

/// Hard upper bound on the kernel cpu_id we'll route to. The BSS-backed per-CPU storage in
/// callers is sized with this constant, so it must be a compile-time value — but only the
/// first `getNumCPUs()` shards are used at runtime (and only those get faulted in).
constexpr uint32_t MAX_CPUS = 1024;

/// Runtime CPU count, capped at `MAX_CPUS`. Cached on first call.
/// Returns `min(get_nprocs_conf(), MAX_CPUS)`, or 1 on non-Linux.
uint32_t getNumCPUs() noexcept;

/// Current CPU id via `sched_getcpu` (read from TLS).
/// Returns -1 on error or non-Linux.
ALWAYS_INLINE inline int32_t getCurrentCPU()
{
#if defined(OS_LINUX)
    return sched_getcpu();
#else
    return -1;
#endif
}

}
