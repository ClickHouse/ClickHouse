#pragma once

#include <cstddef>

/// Size in bytes of the current CPU's data cache at `level`, or 0 when it cannot be probed.
/// L1 is always split, so only a data cache counts there; from L2 up a unified cache counts too.
/// Read from CPUID on x86_64, from sysfs on other Linux, and from `sysctl` on Darwin.
size_t getCPUDataCacheSize(unsigned level);
