#pragma once

#include <cstddef>

/// L2 data/unified cache size in bytes for the current CPU. Read from CPUID on
/// x86_64 (Intel leaf 4 / AMD 0x8000001D), from sysfs on non-x86 Linux, and
/// from `sysctl` on Darwin; defaults to 256 KiB if probing fails.
/// Computed once and cached.
size_t getL2CacheSize();
