#pragma once

#include <optional>
#include <filesystem>
#include <string_view>


#if defined(OS_LINUX)
/// I think it is possible to mount the cgroups hierarchy somewhere else (e.g. when in containers).
/// /sys/fs/cgroup was still symlinked to the actual mount in the cases that I have seen.
static inline const std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";
#endif

/// Is cgroups v2 enabled on the system?
bool cgroupsV2Enabled();

/// Detects which cgroup v2 the process belongs to and returns its filesystem path.
/// Assumes that cgroupsV2Enabled() was called prior and returned true.
/// If the cgroup cannot be determined, an empty path is returned.
std::filesystem::path cgroupV2PathOfProcess();

/// Returns the most nested cgroup dir containing the specified file.
/// If cgroups v2 is not enabled - returns an empty optional.
std::optional<std::string> getCgroupsV2PathContainingFile(std::string_view file_name);
