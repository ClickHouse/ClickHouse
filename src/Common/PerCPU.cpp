#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
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

}
