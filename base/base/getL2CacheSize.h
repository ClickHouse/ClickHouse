#pragma once

#include <cstddef>

/// L2 data/unified cache size in bytes for the current CPU, defaulting to 256 KiB when probing
/// fails. Computed once and cached.
size_t getL2CacheSize();
