#pragma once

#include <cstddef>

/// L2 data or unified cache size in bytes for the current CPU, or 256 KiB when it cannot be read.
/// Read once and cached.
size_t getL2CacheSize();
