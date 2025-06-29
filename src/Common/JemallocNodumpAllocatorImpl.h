#pragma once

#include "config.h"

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>

namespace DB
{

class JemallocNodumpAllocatorImpl
{
public:
    static JemallocNodumpAllocatorImpl & instance();

    JemallocNodumpAllocatorImpl(const JemallocNodumpAllocatorImpl &) = delete;
    JemallocNodumpAllocatorImpl & operator =(const JemallocNodumpAllocatorImpl &) = delete;

    void * allocate(size_t size, size_t alignment = 0) const;
    void deallocate(void * p) const;


private:
    inline static extent_hooks_t extent_hooks_{};
    inline static extent_alloc_t * original_alloc_ = nullptr;

    /// Used to allocate extents by jemalloc.
    static void * alloc(
        extent_hooks_t * extent, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind);

    explicit JemallocNodumpAllocatorImpl();

    void setupArena();

    unsigned arena_index{0};
    int flags{0};
};
}
#endif
