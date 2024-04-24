#include <base/cgroupsv2.h>

#include <base/defines.h>

#include <fstream>
#include <sstream>


bool cgroupsV2Enabled()
{
#if defined(OS_LINUX)
    /// This file exists iff the host has cgroups v2 enabled.
    auto controllers_file = default_cgroups_mount / "cgroup.controllers";
    if (!std::filesystem::exists(controllers_file))
        return false;
    return true;
#else
    return false;
#endif
}

bool cgroupsV2MemoryControllerEnabled()
{
#if defined(OS_LINUX)
    chassert(cgroupsV2Enabled());

    /// According to https://docs.kernel.org/admin-guide/cgroup-v2.html:
    /// - file 'cgroup.controllers' defines which controllers *can* be enabled
    /// - file 'cgroup.subtree_control' defines which controllers *are* enabled

    /// Check from the bottom-most nested subtree_control file. If it does not contain the memory controller,
    /// check the parent levels recursively as nested subtree_control files can only contain more controllers
    /// than their parent subtree_control file.
    std::string cgroup = cgroupV2OfProcess();
    auto current_cgroup = cgroup.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup);

    while (current_cgroup != default_cgroups_mount.parent_path())
    {
        std::ifstream subtree_control_file(current_cgroup / "cgroup.subtree_control");
        if (!subtree_control_file.is_open())
            return false;
        std::string controllers;
        std::getline(subtree_control_file, controllers);
        if (controllers.find("memory") != std::string::npos)
            return true;
        current_cgroup = current_cgroup.parent_path();
    }
    return false;
#else
    return false;
#endif
}

std::string cgroupV2OfProcess()
{
#if defined(OS_LINUX)
    chassert(cgroupsV2Enabled());
    /// All PIDs assigned to a cgroup are in /sys/fs/cgroups/{cgroup_name}/cgroup.procs
    /// A simpler way to get the membership is:
    std::ifstream cgroup_name_file("/proc/self/cgroup");
    if (!cgroup_name_file.is_open())
        return "";
    /// With cgroups v2, there will be a *single* line with prefix "0::/"
    /// (see https://docs.kernel.org/admin-guide/cgroup-v2.html)
    std::string cgroup;
    std::getline(cgroup_name_file, cgroup);
    static const std::string v2_prefix = "0::/";
    if (!cgroup.starts_with(v2_prefix))
        return "";
    cgroup = cgroup.substr(v2_prefix.length());
    return cgroup;
#else
    return "";
#endif
}
