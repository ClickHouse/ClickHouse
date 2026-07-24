#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#include <cstddef>

/// The rseq area location of the initial registration, exported by glibc >= 2.35. Declared weak
/// so the binary also links against older/other libcs, where the address resolves to null.
/// `__rseq_size` is 0 when registration was disabled (the `glibc.pthread.rseq` tunable) or failed.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
extern "C" const ptrdiff_t __rseq_offset __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
extern "C" const unsigned int __rseq_size __attribute__((weak)); // NOLINT(bugprone-reserved-identifier,cert-dcl37-c,cert-dcl51-cpp)
#pragma clang diagnostic pop
#elif defined(OS_DARWIN)
#include <unistd.h>
#endif

#include <algorithm>

namespace PerCPU
{

UInt32 getNumCPUs() noexcept
{
    static const UInt32 cached = []
    {
#if defined(OS_LINUX)
        const Int64 n = get_nprocs_conf();
#elif defined(OS_DARWIN)
        const Int64 n = ::sysconf(_SC_NPROCESSORS_ONLN);
#else
        /// `getCurrentCPU` is not implemented here, so per-CPU routing is impossible; report one
        /// CPU so callers size a single shard instead of creating unreachable ones (e.g. FreeBSD).
        const Int64 n = 1;
#endif
        if (n <= 0)
            return UInt32{1};
        return std::min(static_cast<UInt32>(n), MAX_CPUS);
    }();
    return cached;
}

bool haveRSeq() noexcept
{
#if defined(OS_LINUX)
    /// The registered area must cover at least the `cpu_id` field (offset 4, size 4).
    if (&__rseq_size == nullptr || __rseq_size < 8)
        return false;
    /// The kernel uses negative sentinels in `cpu_id`: -1 (UNINITIALIZED) and -2
    /// (REGISTRATION_FAILED). The production `sched_getcpu`
    /// (base/glibc-compatibility/musl/sched_getcpu.c) rejects them before taking the rseq
    /// fast path, so a thread in these states is also on the slow fallback and must be
    /// reported as not having rseq.
    const char * tp = static_cast<const char *>(__builtin_thread_pointer());
    return static_cast<int32_t>(*reinterpret_cast<const volatile uint32_t *>(tp + __rseq_offset + 4)) >= 0;
#else
    return false;
#endif
}

}
