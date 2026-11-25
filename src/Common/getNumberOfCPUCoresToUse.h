#pragma once

#include <cstdint>
#include <optional>
#include <string>

/// Get the number of CPU cores to use. Depending on the machine size we choose
/// between the number of physical and logical cores.
/// Also under cgroups we respect possible cgroups limits.
unsigned getNumberOfCPUCoresToUse();

/// Number of online logical CPUs (with SMT/HyperThreading), read from the kernel on Linux so that the
/// value does not depend on the libc: glibc and musl disagree about `sysconf(_SC_NPROCESSORS_ONLN)`
/// for a process with a restricted affinity mask. Ignores cgroup limits and affinity.
unsigned getNumberOfLogicalCPUCores();

/// Parse a kernel cpu-list such as "0-15" or "0,2-4,7" (the format of /sys/devices/system/cpu/* and
/// cgroup cpuset files) and return the number of CPUs in it, or nullopt if the text is not a cpu-list.
std::optional<uint32_t> countCPUsInList(const std::string & cpu_list);
