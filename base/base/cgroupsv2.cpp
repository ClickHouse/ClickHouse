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
    /// Caveat: nested groups may disable controllers. For simplicity, check only the top-level group.
    std::ifstream subtree_control_file(default_cgroups_mount / "cgroup.subtree_control");
    if (!subtree_control_file.is_open())
        return false;
    std::string controllers;
    std::getline(subtree_control_file, controllers);
    if (controllers.find("memory") == std::string::npos)
        return false;
    return true;
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
