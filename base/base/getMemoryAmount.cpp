#include <base/getMemoryAmount.h>

#include <base/getPageSize.h>

#include <fstream>
#include <sstream>
#include <stdexcept>

#include <unistd.h>
#include <sys/types.h>
#include <sys/param.h>
#if defined(BSD)
#include <sys/sysctl.h>
#endif


namespace
{

std::optional<uint64_t> getCgroupsV2MemoryLimit()
{
#if defined(OS_LINUX)
    const std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";

    /// This file exists iff the host has cgroups v2 enabled.
    std::ifstream controllers_file(default_cgroups_mount / "cgroup.controllers");
    if (!controllers_file.is_open())
        return {};

    /// Make sure that the memory controller is enabled.
    /// - cgroup.controllers defines which controllers *can* be enabled.
    /// - cgroup.subtree_control defines which controllers *are* enabled.
    /// (see https://docs.kernel.org/admin-guide/cgroup-v2.html)
    /// Caveat: nested groups may disable controllers. For simplicity, check only the top-level group.
    /// ReadBufferFromFile subtree_control_file(default_cgroups_mount / "cgroup.subtree_control");
    /// std::string subtree_control;
    /// readString(subtree_control, subtree_control_file);
    /// if (subtree_control.find("memory") == std::string::npos)
    ///     return {};
    std::ifstream subtree_control_file(default_cgroups_mount / "cgroup.subtree_control");
    std::stringstream subtree_control_buf;
    subtree_control_buf << subtree_control_file.rdbuf();
    std::string subtree_control = subtree_control_buf.str();
    if (subtree_control.find("memory") == std::string::npos)
        return {};

    /// Identify the cgroup the process belongs to
    /// All PIDs assigned to a cgroup are in /sys/fs/cgroups/{cgroup_name}/cgroup.procs
    /// A simpler way to get the membership is:
    std::ifstream cgroup_name_file("/proc/self/cgroup");
    if (!cgroup_name_file.is_open())
        return {};

    std::stringstream cgroup_name_buf;
    cgroup_name_buf << cgroup_name_file.rdbuf();
    std::string cgroup_name = cgroup_name_buf.str();
    if (!cgroup_name.empty() && cgroup_name.back() == '\n')
        cgroup_name.pop_back(); /// remove trailing newline, if any
    /// With cgroups v2, there will be a *single* line with prefix "0::/"
    const std::string v2_prefix = "0::/";
    if (!cgroup_name.starts_with(v2_prefix))
        return {};
    cgroup_name = cgroup_name.substr(v2_prefix.length());

    std::filesystem::path current_cgroup = cgroup_name.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup_name);

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

/** Returns the size of physical memory (RAM) in bytes.
  * Returns 0 on unsupported platform
  */
uint64_t getMemoryAmountOrZero()
{
    int64_t num_pages = sysconf(_SC_PHYS_PAGES);
    if (num_pages <= 0)
        return 0;

    int64_t page_size = getPageSize();
    if (page_size <= 0)
        return 0;

    uint64_t memory_amount = num_pages * page_size;

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
                if (limit_v1 < memory_amount)
                    memory_amount = limit_v1;
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
