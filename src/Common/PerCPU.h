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

/// Current CPU id via `sched_getcpu`.
/// Returns -1 on error or non-Linux.
ALWAYS_INLINE inline int32_t getCurrentCPU()
{
#if defined(OS_LINUX) && defined(USE_MUSL)
    /// glibc's `sched_getcpu` is a ~1ns rseq TLS read, cheap enough to call on every
    /// `ProfileEvents` increment. musl has no rseq support: `sched_getcpu` is a vDSO
    /// call on x86_64 and a full syscall on aarch64 (no vDSO `getcpu` there), which
    /// made every counter increment pay ~300ns and regressed counter-dense queries
    /// by tens of percent on aarch64. Cache the id per thread and refresh it
    /// periodically: per-CPU consumers aggregate all shards on read, so stale
    /// attribution after a migration only costs some sharding precision for up to
    /// `refresh_period` calls, never correctness. Plain thread-locals keep this
    /// async-signal-safe (a signal between the refresh steps at worst repeats it).
    constexpr uint32_t refresh_period = 256;
    static thread_local uint32_t calls_until_refresh = 0;
    static thread_local int32_t cached_cpu = -1;
    if (calls_until_refresh == 0)
    {
        cached_cpu = sched_getcpu();
        calls_until_refresh = refresh_period;
    }
    --calls_until_refresh;
    return cached_cpu;
#elif defined(OS_LINUX)
    return sched_getcpu();
#else
    return -1;
#endif
}

}
