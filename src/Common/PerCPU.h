#pragma once

#include <base/defines.h>
#include <base/types.h>

#if defined(OS_FREEBSD) && defined(__x86_64__)
#include <sched.h>
#endif

namespace PerCPU
{

/// Hard upper bound on the kernel cpu_id we'll route to. The BSS-backed per-CPU storage in
/// callers is sized with this constant, so it must be a compile-time value — but only the
/// first `getNumCPUs()` shards are used at runtime (and only those get faulted in).
constexpr UInt32 MAX_CPUS = 1024;

/// Whether `getCurrentCPU` is implemented for this platform/arch; when false it always returns
/// -1 and callers must collapse their per-CPU sharding to a single shard themselves.
constexpr bool HAS_GET_CURRENT_CPU =
#if defined(OS_LINUX) || defined(OS_DARWIN) || (defined(OS_FREEBSD) && defined(__x86_64__))
    true;
#else
    false;
#endif

/// Number of CPUs in the machine — a fact independent of `HAS_GET_CURRENT_CPU` — capped at
/// `MAX_CPUS`; 1 if the count is unavailable. Cached on first call. `get_nprocs_conf()` on
/// Linux, `sysconf(_SC_NPROCESSORS_ONLN)` on Darwin and FreeBSD.
UInt32 getNumCPUs() noexcept;

#if defined(OS_LINUX)
namespace detail
{
/// Body of `getCurrentCPU`: an rseq-area read with lazy per-thread registration (see
/// PerCPU.cpp), falling back to `sched_getcpu` for threads without rseq. ALWAYS_INLINE
/// takes effect under (Thin)LTO; plain builds keep the call.
ALWAYS_INLINE Int32 getCurrentCPULinux() noexcept;
}
#endif

/// Current CPU id, or -1 if unavailable (callers must treat a negative value as "unknown" and
/// fall back to a fixed shard). The id is not guaranteed to be dense in [0, getNumCPUs()); callers
/// bound it (`cpu % N` or `cpu < N ? cpu : 0`). Cheap on every supported platform: a TLS read on
/// Linux with rseq (plus a one-time registration syscall per thread), a register read on Darwin
/// and FreeBSD/amd64.
/// Only Linux threads without rseq (kernel < 4.18, seccomp) pay the `sched_getcpu` fallback —
/// the `getcpu` vDSO entry on x86_64, a real syscall on AArch64.
ALWAYS_INLINE inline Int32 getCurrentCPU()
{
#if defined(OS_LINUX)
    return detail::getCurrentCPULinux();
#elif defined(OS_FREEBSD) && defined(__x86_64__)
    /// libc ifunc resolving to RDPID/RDTSCP (the amd64 kernel maintains TSC_AUX = cpu id): a
    /// register read, not a syscall. The syscall resolver is picked only when CPUID lacks
    /// RDTSCP; every SSE4.2-capable CPU also has RDTSCP and ClickHouse refuses to start
    /// without SSE4.2, so reaching it needs a hypervisor masking RDTSCP — slower, still
    /// correct. Other FreeBSD arches have only the syscall flavor (sched_getcpu_gen), too
    /// slow for per-CPU routing, so they stay on the -1 path.
    return sched_getcpu();
#elif defined(OS_DARWIN) && defined(__aarch64__)
    /// macOS has no `sched_getcpu`. XNU exposes the current CPU number to userspace in the low 12
    /// bits of a per-CPU register, extracted exactly as libsyscall's `_os_cpu_number` (up to 4096
    /// CPUs): https://github.com/apple-oss-distributions/xnu/blob/1031c584a5e37aff177559b9f69dbd3c8c3fd30a/libsyscall/os/tsd.h
    /// The layout is Apple-internal and documented there as "subject to change"; e.g. macOS 11
    /// kept the CPU number in `TPIDRRO_EL0` instead, so there this reads unrelated TLS bits. Callers
    /// bound the value, so on such systems the worst case is degraded sharding, not incorrectness.
    UInt64 tpidr;
    __asm__ volatile("mrs %0, TPIDR_EL0" : "=r"(tpidr));
    return static_cast<Int32>(tpidr & 0xfff);
#elif defined(OS_DARWIN) && defined(__x86_64__)
    /// Same source as above (`_os_cpu_number`): XNU encodes the CPU number in the low 12 bits of the
    /// per-CPU IDTR *limit* (the first word `sidt` stores: 16-bit limit + low 48 bits of the base),
    /// so masking the first word matches Apple's implementation exactly.
    struct { UInt64 limit_and_base_low; UInt64 base_high; } idtr;
    __asm__ volatile("sidt %0" : "=m"(idtr));
    return static_cast<Int32>(idtr.limit_and_base_low & 0xfff);
#else
    static_assert(!HAS_GET_CURRENT_CPU);
    return -1;
#endif
}

/// Whether the current thread's `getCurrentCPU` is backed by an rseq area: glibc's registration
/// (glibc 2.35+ with the `glibc.pthread.rseq` tunable enabled), or the area we register ourselves
/// when glibc did not — older glibc at runtime, or the tunable disabled (documented as leaving
/// rseq for the application to manage). Attempts this thread's lazy registration if it has not
/// happened yet. Threads without rseq take a slower `getCurrentCPU` fallback — on AArch64 a real
/// syscall, since there is no `getcpu` vDSO entry — making per-CPU routing costly there.
bool haveRSeq() noexcept;

}
