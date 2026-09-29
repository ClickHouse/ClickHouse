#include <Common/getNumberOfCPUCoresToUse.h>

#if defined(OS_LINUX)
#    include <cmath>
#    include <fstream>
#endif

#include <boost/algorithm/string/trim.hpp>
#include <boost/algorithm/string/split.hpp>
#include <base/cgroupsv2.h>
#include <base/range.h>

#include <filesystem>
#include <thread>
#include <set>
#include <charconv>
#include <optional>
#include <vector>

#if defined(OS_LINUX)
namespace
{
    std::optional<uint32_t> countCPUsInList(const std::string & cpu_list);
}
#endif

unsigned getNumberOfLogicalCPUCores()
{
    static const unsigned cores = []
    {
#if defined(OS_LINUX)
        /// Read the kernel's own list of online CPUs. This is what glibc's sysconf(_SC_NPROCESSORS_ONLN) does.
        /// musl derives the same sysconf value from the calling thread's affinity mask instead, so a server started
        /// under `taskset` would see a different core count depending on the libc it was linked against, and every
        /// `auto` setting and thread pool sized from it would differ. Reading sysfs directly makes the result
        /// libc-independent. Cgroup CPU limits are applied on top by getNumberOfCPUCoresToUse.
        std::ifstream online("/sys/devices/system/cpu/online");
        std::string line;
        if (online.is_open() && std::getline(online, line))
            if (auto count = countCPUsInList(line))
                return *count;
        /// /sys is not mounted (chroot) or has an unexpected format.
#endif
        return std::thread::hardware_concurrency();
    }();
    return cores;
}

namespace
{

#if defined(OS_LINUX)
int32_t readFrom(const std::filesystem::path & filename, int default_value)
{
    std::ifstream infile(filename);
    if (!infile.is_open())
        return default_value;
    int idata = 0;
    if (infile >> idata)
        return idata;
    return default_value;
}

/// Parse a kernel cpu-list such as "0-15" or "0,2-4,7" (the format of /sys/devices/system/cpu/* and
/// cgroup cpuset files) and return the number of CPUs in it, or nullopt if the text is not a cpu-list.
std::optional<uint32_t> countCPUsInList(const std::string & cpu_list)
{
    if (cpu_list.empty())
        return std::nullopt;

    auto parse = [](const std::string & text, int & value)
    {
        return std::from_chars(text.data(), text.data() + text.size(), value).ec == std::errc{};
    };

    std::vector<std::string> cpu_ranges;
    boost::split(cpu_ranges, cpu_list, boost::is_any_of(","));
    uint32_t cpus_count = 0;
    for (const std::string & cpu_number_or_range : cpu_ranges)
    {
        std::vector<std::string> cpu_range;
        boost::split(cpu_range, cpu_number_or_range, boost::is_any_of("-"));

        int start = 0;
        int end = 0;
        if (cpu_range.size() == 1 && parse(cpu_range[0], start))
            end = start;
        else if (cpu_range.size() != 2 || !parse(cpu_range[0], start) || !parse(cpu_range[1], end) || end < start)
            return std::nullopt;

        cpus_count += static_cast<uint32_t>(end - start) + 1;
    }
    return cpus_count;
}

/// Try to look at cgroups limit if it is available.
uint32_t getCGroupLimitedCPUCores(unsigned default_cpu_count)
{
    uint32_t quota_count = default_cpu_count;
    /// cgroupsv2
    if (cgroupsV2Enabled())
    {
        /// First, we identify the path of the cgroup the process belongs
        std::filesystem::path cgroup_path = cgroupV2PathOfProcess();
        if (cgroup_path.empty())
            return default_cpu_count;

        auto current_cgroup = cgroup_path;

        // Looking for cpu.max in directories from the current cgroup to the top level
        // It does not stop on the first time since the child could have a greater value than parent
        while (current_cgroup != default_cgroups_mount.parent_path())
        {
            std::ifstream cpu_max_file(current_cgroup / "cpu.max");
            if (cpu_max_file.is_open())
            {
                std::string cpu_limit_str;
                float cpu_period = 0;
                cpu_max_file >> cpu_limit_str >> cpu_period;
                if (cpu_limit_str != "max" && cpu_period != 0)
                {
                    float cpu_limit = std::stof(cpu_limit_str);
                    quota_count = std::min(static_cast<uint32_t>(ceil(cpu_limit / cpu_period)), quota_count);
                }
            }
            current_cgroup = current_cgroup.parent_path();
        }
        current_cgroup = cgroup_path;
        // Looking for cpuset.cpus.effective in directories from the current cgroup to the top level
        while (current_cgroup != default_cgroups_mount.parent_path())
        {
            std::ifstream cpuset_cpus_file(current_cgroup / "cpuset.cpus.effective");
            current_cgroup = current_cgroup.parent_path();
            if (cpuset_cpus_file.is_open())
            {
                // The line in the file is "0,2-4,6,9-14" cpu numbers
                // It's always grouped and ordered
                std::string cpuset_line;
                cpuset_cpus_file >> cpuset_line;
                auto cpus_count = countCPUsInList(cpuset_line);
                if (!cpus_count)
                    continue;
                quota_count = std::min(*cpus_count, quota_count);
                break;
            }
        }
        return quota_count;
    }
    /// cgroupsv1
    /// Return the number of milliseconds per period process is guaranteed to run.
    /// -1 for no quota
    int cgroup_quota = readFrom(default_cgroups_mount / "cpu/cpu.cfs_quota_us", -1);
    int cgroup_period = readFrom(default_cgroups_mount / "cpu/cpu.cfs_period_us", -1);
    if (cgroup_quota > -1 && cgroup_period > 0)
        quota_count = static_cast<uint32_t>(ceil(static_cast<float>(cgroup_quota) / static_cast<float>(cgroup_period)));

    return std::min(default_cpu_count, quota_count);
}
#endif

/// Returns number of physical cores, unlike getNumberOfLogicalCPUCores which returns the logical core count. With 2-way SMT
/// (HyperThreading) enabled, physical_concurrency() returns half of getNumberOfLogicalCPUCores(), otherwise return the same.
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
        return getNumberOfLogicalCPUCores();

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

        if (key.contains("physical id"))
        {
            cur_core_entry.first = std::stoi(val);
            continue;
        }

        if (key.contains("core id"))
        {
            cur_core_entry.second = std::stoi(val);
            core_entries.insert(cur_core_entry);
            continue;
        }
    }
    return core_entries.empty() ? /*unexpected format*/ getNumberOfLogicalCPUCores() : static_cast<unsigned>(core_entries.size());
}
catch (const std::exception &)
{
    return getNumberOfLogicalCPUCores(); /// parsing error
}
#endif

unsigned getNumberOfCPUCoresToUseImpl()
{
    unsigned cores = getNumberOfLogicalCPUCores(); /// logical cores (with SMT/HyperThreading)

#if defined(__x86_64__) && defined(OS_LINUX)
    /// Most x86_64 CPUs have 2-way SMT (Hyper-Threading).
    /// Aarch64 and RISC-V don't have SMT so far.
    /// POWER has SMT and it can be multi-way (e.g. 8-way), but we don't know how ClickHouse really behaves, so use all of them.
    ///
    /// On really big machines, SMT is detrimental to performance (+ ~5% overhead in ClickBench). On such machines, we limit ourself to the physical cores.
    /// Few cores indicate it is a small machine, runs in a VM or is a limited cloud instance --> it is reasonable to use all the cores.
    if (cores >= 64)
        cores = physical_concurrency();
#endif

#if defined(OS_LINUX)
    cores = getCGroupLimitedCPUCores(cores);
#endif

    return cores;
}

}

unsigned getNumberOfCPUCoresToUse()
{
    /// Calculate once.
    static const unsigned cores = getNumberOfCPUCoresToUseImpl();
    return cores;
}
