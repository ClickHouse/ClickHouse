#pragma once

#include <filesystem>
#include <string>

#if defined(OS_LINUX)
/// I think it is possible to mount the cgroups hierarchy somewhere else (e.g. when in containers).
/// /sys/fs/cgroup was still symlinked to the actual mount in the cases that I have seen.
static inline const std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";
#endif

/// Is cgroups v2 enabled on the system?
bool cgroupsV2Enabled();

/// Is the memory controller of cgroups v2 enabled on the system?
/// Assumes that cgroupsV2Enabled() is enabled.
bool cgroupsV2MemoryControllerEnabled();

/// Which cgroup does the process belong to?
/// Returns an empty string if the cgroup cannot be determined.
/// Assumes that cgroupsV2Enabled() is enabled.
std::string cgroupV2OfProcess();
