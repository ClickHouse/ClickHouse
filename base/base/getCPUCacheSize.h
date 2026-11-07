#pragma once

#include <cstddef>

/// Size in bytes of the current CPU's data cache at `level`, or 0 when it cannot be read.
/// At level 1 only a data cache counts. From level 2 up, a unified cache counts too.
/// Read from CPUID on x86_64, otherwise from sysfs on Linux and from `sysctl` on Darwin.
/// `sysctl` knows only levels 1 to 3. Each call reads the value again.
size_t getCPUDataCacheSize(unsigned level);
