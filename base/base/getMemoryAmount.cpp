#include <base/getMemoryAmount.h>

#include <base/cgroupsv2.h>
#include <base/getPageSize.h>
#include <base/Numa.h>

#include <algorithm>
#include <fstream>
#include <optional>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#endif

namespace
{

std::optional<uint64_t> getCgroupsV2MemoryLimit()
{
#if defined(OS_LINUX)
    if (!cgroupsV2Enabled())
        return {};

    std::filesystem::path current_cgroup = cgroupV2PathOfProcess();
    if (current_cgroup.empty())
        return {};

    /// Open the bottom-most nested memory limit setting file. If there is no such file at the current
    /// level, try again at the parent level as memory settings are inherited.
    while (current_cgroup != default_cgroups_mount.parent_path())
    {
        std::ifstream setting_file(current_cgroup / "memory.max");
        if (setting_file.is_open())
        {
            uint64_t value = {};
            if (setting_file >> value)
                return {value};
            return {}; /// e.g. the cgroups default "max"
        }
        current_cgroup = current_cgroup.parent_path();
    }

    return {};
#else
    return {};
#endif
}

std::optional<uint64_t> getWindowsJobObjectMemoryLimit()
{
#if defined(OS_WINDOWS)
    /// Windows has no cgroups. A container, and anything else that wants to cap a process, uses
    /// a job object instead, and `GlobalMemoryStatusEx` does not see that cap - it reports what
    /// the machine has. A null handle asks about the job the calling process belongs to; the
    /// call fails when there is no such job, which is the ordinary case outside a container.
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION info{};
    if (!QueryInformationJobObject(nullptr, JobObjectExtendedLimitInformation, &info, sizeof(info), nullptr))
        return {};

    return windowsJobObjectMemoryLimit(
        info.BasicLimitInformation.LimitFlags,
        static_cast<uint64_t>(info.JobMemoryLimit),
        static_cast<uint64_t>(info.ProcessMemoryLimit));
#else
    return {};
#endif
}

}

#if defined(OS_WINDOWS)
std::optional<uint64_t> windowsJobObjectMemoryLimit(uint32_t limit_flags, uint64_t job_memory_limit, uint64_t process_memory_limit)
{
    std::optional<uint64_t> limit;

    /// The job-wide limit is what container runtimes set, and the per-process limit caps this
    /// process alone. Either one is an upper bound on what this process can commit, so when both
    /// are present the smaller one is the effective limit. A flag that is not set leaves the
    /// corresponding field meaningless, so it must not be read.
    if (limit_flags & JOB_OBJECT_LIMIT_JOB_MEMORY)
        limit = job_memory_limit;
    if (limit_flags & JOB_OBJECT_LIMIT_PROCESS_MEMORY)
        limit = limit.has_value() ? std::min(*limit, process_memory_limit) : process_memory_limit;

    return limit;
}
#endif

uint64_t getMemoryAmountOrZero()
{
#if defined(OS_WINDOWS)
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    if (!GlobalMemoryStatusEx(&status))
        return 0;

    uint64_t memory_amount = status.ullTotalPhys;
#else
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    uint64_t memory_amount = num_pages * page_size;
#endif

    if (auto total_numa_memory = DB::getNumaNodesTotalMemory(); total_numa_memory.has_value())
        memory_amount = *total_numa_memory;

    /// Respect the memory limit of the job object this process belongs to. This is the Windows
    /// counterpart of the cgroup clamp below: without it every caller that sizes itself from
    /// `getMemoryAmount` - the default `max_server_memory_usage` in `clickhouse-local`, the
    /// background pools in `Context::initializeBackgroundExecutorsIfNeeded` - would budget for
    /// the whole machine inside a container and be killed by the outer quota instead of
    /// staying under it.
    if (auto job_limit = getWindowsJobObjectMemoryLimit(); job_limit.has_value() && *job_limit < memory_amount)
        memory_amount = *job_limit;

    /// Respect the memory limit set by cgroups v2.
    auto limit_v2 = getCgroupsV2MemoryLimit();
    if (limit_v2.has_value() && *limit_v2 < memory_amount)
         memory_amount = *limit_v2;
    else
    {
        /// Cgroups v1 were replaced by v2 in 2015. The only reason we keep supporting v1 is that the transition to v2
        /// has been slow. Caveat : Hierarchical groups as in v2 are not supported for v1, the location of the memory
        /// limit (virtual) file is hard-coded.
        /// TODO: check at the end of 2024 if we can get rid of v1.
        std::ifstream limit_file_v1("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        if (limit_file_v1.is_open())
        {
            uint64_t limit_v1 = {};
            if (limit_file_v1 >> limit_v1)
                memory_amount = std::min(memory_amount, limit_v1);
        }
    }

    return memory_amount;
}


uint64_t getMemoryAmount()
{
    auto res = getMemoryAmountOrZero();
    if (!res)
        throw std::runtime_error("Cannot determine memory amount");
    return res;
}
