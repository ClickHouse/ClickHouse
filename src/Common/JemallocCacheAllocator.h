#pragma once

#include "config.h"

#if USE_JEMALLOC

#include <cstddef>

/// Allocator that directs all allocations to a dedicated jemalloc arena for cache data.
/// Interface matches Allocator<false, false> for use as a drop-in PODArray / Memory<> template parameter.
/// Uses MALLOCX_TCACHE_NONE to ensure strict arena affinity (no thread-cache cross-pollution).
/// Cache allocations are infrequent (per mark-file, not per-row), so tcache bypass has no perf impact.
class JemallocCacheAllocator
{
public:
    void * alloc(size_t size, size_t alignment = 0);
    void free(void * buf, size_t size);
    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0);

protected:
    static constexpr size_t getStackThreshold() { return 0; }
    static constexpr bool clear_memory = false;
};

#else

#include <Common/Allocator.h>
using JemallocCacheAllocator = Allocator<false, false>;

#endif
