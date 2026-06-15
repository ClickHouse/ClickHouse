#pragma once

#include <cstddef>

/// L2 data/unified cache size in bytes for the current CPU. Read from CPUID on
/// x86_64 (Intel leaf 4 / AMD 0x8000001D); defaults to 256 KiB elsewhere.
/// Computed once and cached.
size_t getL2CacheSize();
