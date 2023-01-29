#include "getNumberOfPhysicalCPUCores.h"

#include "config.h"
#if defined(OS_LINUX)
#    include <cmath>
#    include <fstream>
#endif

#include <boost/algorithm/string/trim.hpp>

#include <thread>
#include <set>

namespace
{

#if defined(OS_LINUX)
int32_t readFrom(const char * filename, int default_value)
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
uint32_t getCGroupLimitedCPUCores(unsigned default_cpu_count)
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

/// Returns number of physical cores, unlike std::thread::hardware_concurrency() which returns the logical core count. With 2-way SMT
/// (HyperThreading) enabled, physical_concurrency() returns half of of std::thread::hardware_concurrency(), otherwise return the same.
#if defined(__x86_64__) && defined(OS_LINUX)
unsigned physical_concurrency()
try
{
    /// The CPUID instruction isn't reliable across different vendors and CPU models. The best option to get the physical core count is
    /// to parse /proc/cpuinfo. boost::thread::physical_concurrency() does the same, so use their implementation.
    ///
    /// See https://doc.callmematthi.eu/static/webArticles/Understanding%20Linux%20_proc_cpuinfo.pdf
    std::ifstream proc_cpuinfo("/proc/cpuinfo");

    if (!proc_cpuinfo.is_open())
        /// In obscure cases (chroot) /proc can be unmounted
        return std::thread::hardware_concurrency();

    using CoreEntry = std::pair<size_t, size_t>; /// physical id, core id
    using CoreEntrySet = std::set<CoreEntry>;

    CoreEntrySet core_entries;

    CoreEntry cur_core_entry;
    std::string line;

    while (std::getline(proc_cpuinfo, line))
    {
        size_t pos = line.find(std::string(":"));
        if (pos == std::string::npos)
            continue;

        std::string key = line.substr(0, pos);
        std::string val = line.substr(pos + 1);

        if (key.find("physical id") != std::string::npos)
        {
            cur_core_entry.first = std::stoi(val);
            continue;
        }

        if (key.find("core id") != std::string::npos)
        {
            cur_core_entry.second = std::stoi(val);
            core_entries.insert(cur_core_entry);
            continue;
        }
    }
    return core_entries.empty() ? /*unexpected format*/ std::thread::hardware_concurrency() : static_cast<unsigned>(core_entries.size());
}
catch (...)
{
    return std::thread::hardware_concurrency(); /// parsing error
}
#endif

unsigned getNumberOfPhysicalCPUCoresImpl()
{
    unsigned cpu_count = std::thread::hardware_concurrency(); /// logical cores (with SMT/HyperThreading)

    /// Most x86_64 CPUs have 2-way SMT (Hyper-Threading).
    /// Aarch64 and RISC-V don't have SMT so far.
    /// POWER has SMT and it can be multi-way (e.g. 8-way), but we don't know how ClickHouse really behaves, so use all of them.

#if defined(__x86_64__) && defined(OS_LINUX)
    /// On really big machines, SMT is detrimental to performance (+ ~5% overhead in ClickBench). On such machines, we limit ourself to the physical cores.
    /// Few cores indicate it is a small machine, runs in a VM or is a limited cloud instance --> it is reasonable to use all the cores.
    if (cpu_count >= 32)
        cpu_count = physical_concurrency();
#endif

#if defined(OS_LINUX)
    cpu_count = getCGroupLimitedCPUCores(cpu_count);
#endif

    return cpu_count;
}

}

unsigned getNumberOfPhysicalCPUCores()
{
    /// Calculate once.
    static auto res = getNumberOfPhysicalCPUCoresImpl();
    return res;
}
