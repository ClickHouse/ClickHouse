#pragma once

#include <cstddef>

/// Return the data cache size of the second cache level (in bytes) for the
/// current CPU. On x86_64 reads it directly from CPUID (leaf 4 on Intel,
/// 0x8000001D on AMD); falls back to `sysconf(_SC_LEVEL2_CACHE_SIZE)`, then
/// `/sys/devices/system/cpu/cpu0/cache/index2/size`. Sysfs is needed for
/// musl, which does not implement that `sysconf` key (it returns `-1`).
/// Falls back to 256 KiB.
///
/// The value is computed once at first call and cached.
size_t getL2CacheSize();
