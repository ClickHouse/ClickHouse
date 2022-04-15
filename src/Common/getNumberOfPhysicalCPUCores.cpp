#include "getNumberOfPhysicalCPUCores.h"

#include <Common/config.h>
#if defined(OS_LINUX)
#    include <cmath>
#    include <fstream>
#endif
#if USE_CPUID
#    include <libcpuid/libcpuid.h>
#endif

#include <thread>

#if defined(OS_LINUX)
static int readFrom(const char * filename, int default_value)
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
static unsigned getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    unsigned quota_count = default_cpu_count;
    /// Return the number of milliseconds per period process is guaranteed to run.
    /// -1 for no quota
    int cgroup_quota = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = readFrom("/sys/fs/cgroup/cpu/cpu.cfs_period_us", -1);
    if (cgroup_quota > -1 && cgroup_period > 0)
        quota_count = ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period));

    return std::min(default_cpu_count, quota_count);
}
#endif

static unsigned getNumberOfPhysicalCPUCoresImpl()
{
    unsigned cpu_count = 0; // start with an invalid num

#if USE_CPUID
    cpu_raw_data_t raw_data;
    cpu_id_t data;

    /// On Xen VMs, libcpuid returns wrong info (zero number of cores). Fallback to alternative method.
    /// Also, libcpuid does not support some CPUs like AMD Hygon C86 7151.
    /// Also, libcpuid gives strange result on Google Compute Engine VMs.
    /// Example:
    ///  num_cores = 12,            /// number of physical cores on current CPU socket
    ///  total_logical_cpus = 1,    /// total number of logical cores on all sockets
    ///  num_logical_cpus = 24.     /// number of logical cores on current CPU socket
    /// It means two-way hyper-threading (24 / 12), but contradictory, 'total_logical_cpus' == 1.

    if (0 == cpuid_get_raw_data(&raw_data) && 0 == cpu_identify(&raw_data, &data) && data.num_logical_cpus != 0)
        cpu_count = data.num_cores * data.total_logical_cpus / data.num_logical_cpus;
#endif

    /// As a fallback (also for non-x86 architectures) assume there are no hyper-threading on the system.
    /// (Actually, only Aarch64 is supported).
    if (cpu_count == 0)
        cpu_count = std::thread::hardware_concurrency();

#if defined(OS_LINUX)
    /// TODO: add a setting for disabling that, similar to UseContainerSupport in java
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
