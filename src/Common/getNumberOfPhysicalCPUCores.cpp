#include "getNumberOfPhysicalCPUCores.h"

#include "config.h"
#if defined(OS_LINUX)
#    include <cmath>
#    include <fstream>
#endif

#include <thread>

#if defined(OS_LINUX)
static int32_t readFrom(const char * filename, int default_value)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
        return default_value;
    int idata;
    if (infile >> idata)
        return idata;
    else
        return default_value;
}

/// Try to look at cgroups limit if it is available.
static uint32_t getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    uint32_t quota_count = default_cpu_count;
    /// Return the number of milliseconds per period process is guaranteed to run.
    /// -1 for no quota
    int cgroup_quota = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_period_us", -1);
    if (cgroup_quota > -1 && cgroup_period > 0)
        quota_count = static_cast<uint32_t>(ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period)));

    return std::min(default_cpu_count, quota_count);
}
#endif

static unsigned getNumberOfPhysicalCPUCoresImpl()
{
    unsigned cpu_count = std::thread::hardware_concurrency();

#if defined(OS_LINUX)
    cpu_count = getCGroupLimitedCPUCores(cpu_count);
#endif

    return cpu_count;
}

unsigned getNumberOfPhysicalCPUCores()
{
    /// Calculate once.
    static auto res = getNumberOfPhysicalCPUCoresImpl();
    return res;
}
