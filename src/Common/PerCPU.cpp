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
#endif

#include <algorithm>

namespace PerCPU
{

uint32_t getNumCPUs() noexcept
{
#if defined(OS_LINUX)
    static const uint32_t cached = []
    {
        const int n = get_nprocs_conf();
        if (n <= 0)
            return uint32_t{1};
        return std::min(static_cast<uint32_t>(n), MAX_CPUS);
    }();
    return cached;
#else
    return 1;
#endif
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
