#include "getNumberOfPhysicalCPUCores.h"

#include "config.h"
#if defined(OS_LINUX)
#    include <cmath>
#    include <fstream>
#endif

#include <boost/algorithm/string/trim.hpp>

#include <thread>
#include <set>

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

/// Returns number of physical cores. If Intel hyper-threading (2-way SMP) is enabled, then getPhysicalConcurrency() returns half of of
/// std::thread::hardware_concurrency(), otherwise both values are the same.
static uint32_t physical_concurrency()
try
{
    /// Derive physical cores from /proc/cpuinfo. Unfortunately, the CPUID instruction isn't reliable accross different vendors / CPU
    /// models. Implementation based on boost::thread::physical_concurrency(). See
    /// https://doc.callmematthi.eu/static/webArticles/Understanding%20Linux%20_proc_cpuinfo.pdf for semantics of /proc/cpuinfo in the
    /// presence of multiple cores per socket, multiple sockets and multiple hyperthreading cores per physical core.
    std::ifstream proc_cpuinfo("/proc/cpuinfo");

    using CoreEntry = std::pair<size_t, size_t>;

    std::set<CoreEntry> core_entries;
    CoreEntry cur_core_entry;
    std::string line;

    while (std::getline(proc_cpuinfo, line))
    {
        size_t pos = line.find(std::string(":"));
        if (pos == std::string::npos)
            continue;

        std::string key = line.substr(0, pos);
        std::string val = line.substr(pos + 1);

        boost::trim(key);
        boost::trim(val);

        if (key == "physical id")
        {
            cur_core_entry.first = std::stoi(val);
            continue;
        }

        if (key == "core id")
        {
            cur_core_entry.second = std::stoi(val);
            core_entries.insert(cur_core_entry);
            continue;
        }
    }
    return core_entries.empty() ? /*unexpected format*/ std::thread::hardware_concurrency() : static_cast<uint32_t>(core_entries.size());
}
catch (...)
{
    /// parsing error
    return std::thread::hardware_concurrency();
}
#endif

static unsigned getNumberOfPhysicalCPUCoresImpl()
{
    unsigned cpu_count = std::thread::hardware_concurrency(); /// logical cores

    /// Most x86_64 CPUs have 2-way Hyper-Threading
    /// Aarch64 and RISC-V don't have SMT so far.
    /// POWER has SMT and it can be multiple way (like 8-way), but we don't know how ClickHouse really behaves, so use all of them.

#if defined(__x86_64__)
    /// On big machines, Hyper-Threading is detrimental to performance (+ ~5% overhead in ClickBench).
    /// Let's limit ourself to of physical cores.
    /// For few cores - maybe it is a small machine, or runs in a VM or is a limited cloud instance --> it is reasonable to use all the cores.
        if (cpu_count >= 32)
            cpu_count = physical_concurrency();
#endif

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
