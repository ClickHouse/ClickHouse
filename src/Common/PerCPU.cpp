#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <sys/sysinfo.h>
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

}
