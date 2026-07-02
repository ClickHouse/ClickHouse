#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
#include <cstddef>

/// The rseq area location of the initial registration, exported by glibc >= 2.35. Declared weak
/// so the binary also links against older/other libcs, where the address resolves to null.
/// `__rseq_size` is 0 when registration was disabled (the `glibc.pthread.rseq` tunable) or failed.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-identifier"
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
    return &__rseq_size != nullptr && __rseq_size >= 8;
#else
    return false;
#endif
}

}
