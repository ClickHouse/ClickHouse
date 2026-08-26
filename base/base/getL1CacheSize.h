#pragma once

#include <cstddef>

/// L1 data cache size in bytes for the current CPU, defaulting to 32 KiB when probing fails.
/// Computed once and cached.
size_t getL1CacheSize();
