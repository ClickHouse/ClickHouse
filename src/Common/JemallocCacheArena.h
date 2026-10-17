#pragma once

#include "config.h"
#include <cstddef>

namespace DB::JemallocCacheArena
{

/// Returns the jemalloc arena index dedicated to cache allocations
/// (mark cache, uncompressed cache, etc.).
/// Creates the arena on first call (thread-safe via Meyers singleton).
/// Returns 0 (meaning "use default arena selection") if jemalloc is not available.
unsigned getArenaIndex();

/// Purge dirty pages only in the cache arena, returning memory to the OS.
/// No-op if jemalloc is not available.
void purge();

}
