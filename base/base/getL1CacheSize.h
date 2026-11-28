#pragma once

#include <cstddef>

/// L1 data cache size in bytes for the current CPU, or 32 KiB when it cannot be read.
/// Read once and cached.
size_t getL1CacheSize();
