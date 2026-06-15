#pragma once

#include <cstddef>

/// Return the data cache size of the second cache level (in bytes) for the
/// current CPU. On x86_64 reads it directly from CPUID (leaf 4 on Intel,
/// 0x8000001D on AMD), then tries `sysconf(_SC_LEVEL2_CACHE_SIZE)`, and
/// otherwise returns a 256 KiB default.
///
/// There is intentionally no sysfs fallback: glibc implements that `sysconf`
/// key only on x86, so on other architectures (notably aarch64) the callers'
/// heuristics have always run against this 256 KiB default. Reporting the true
/// L2 there would silently retune those heuristics and regress performance —
/// see the comment in getL2CacheSize.cpp.
///
/// The value is computed once at first call and cached.
size_t getL2CacheSize();
