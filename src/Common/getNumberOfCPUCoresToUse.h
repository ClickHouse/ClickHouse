#pragma once

/// Get the number of CPU cores to use. Depending on the machine size we choose
/// between the number of physical and logical cores.
/// Also under cgroups we respect possible cgroups limits.
unsigned getNumberOfCPUCoresToUse();

/// Number of online logical CPUs (with SMT/HyperThreading), read from the kernel on Linux so that the
/// value does not depend on the libc: glibc and musl disagree about `sysconf(_SC_NPROCESSORS_ONLN)`
/// for a process with a restricted affinity mask. Ignores cgroup limits and affinity.
unsigned getNumberOfLogicalCPUCores();
