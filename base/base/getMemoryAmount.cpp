#include <base/getMemoryAmount.h>

#include <base/cgroupsv2.h>
#include <base/getPageSize.h>

#include <fstream>

#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>

#include "config.h"

#if USE_NUMACTL
#include <numa.h>
#endif


namespace
{

std::optional<uint64_t> getCgroupsV2MemoryLimit()
{
#if defined(OS_LINUX)
    if (!cgroupsV2Enabled())
        return {};

    if (!cgroupsV2MemoryControllerEnabled())
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
            uint64_t value;
            if (setting_file >> value)
                return {value};
            else
                return {}; /// e.g. the cgroups default "max"
        }
        current_cgroup = current_cgroup.parent_path();
    }

    return {};
#else
    return {};
#endif
}

}

uint64_t getMemoryAmountOrZero()
{
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    uint64_t memory_amount = num_pages * page_size;

#if USE_NUMACTL
    if (numa_available() != -1)
    {
        auto * membind = numa_get_membind();
        if (!numa_bitmask_equal(membind, numa_all_nodes_ptr))
        {
            uint64_t total_numa_memory = 0;
            auto max_node = numa_max_node();
            for (int i = 0; i <= max_node; ++i)
            {
                if (numa_bitmask_isbitset(membind, i))
                    total_numa_memory += numa_node_size(i, nullptr);
            }

            memory_amount = total_numa_memory;
        }
        numa_bitmask_free(membind);
    }
#endif

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
            uint64_t limit_v1;
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
