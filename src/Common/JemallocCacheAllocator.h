#pragma once

#include "config.h"

#if USE_JEMALLOC

#include <cstddef>

/// Allocator for long-lived cache data (mark cache, uncompressed cache, page cache).
///
/// Without arena isolation, jemalloc places cache allocations on the same pages as short-lived
/// query temporaries. When cache entries are evicted, their pages can't be returned to the OS
/// because other live objects pin them — causing unbounded RSS growth due to fragmentation.
///
/// This allocator directs all allocations to a dedicated jemalloc arena via mallocx/sdallocx
/// with MALLOCX_ARENA(N) | MALLOCX_TCACHE_NONE flags. Cache pages never mix with query-processing
/// allocations, so evicted entries produce fully-reclaimable pages.
///
/// When to use this allocator:
///   1. The data is long-lived and cache-managed (stays resident until LRU/SLRU eviction)
///   2. The container supports a custom allocator template parameter
///      (PODArray<T, N, Allocator>, Memory<Allocator>, or direct alloc()/free() calls)
///
/// When NOT to use:
///   - Short-lived per-query data (e.g. ReverseLookupCache) — same lifetime as query temporaries
///   - Containers with hardcoded allocators (e.g. ColumnPtr, std::string, std::vector)
///
/// Falls back to Allocator<false, false> when jemalloc is not available.
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
