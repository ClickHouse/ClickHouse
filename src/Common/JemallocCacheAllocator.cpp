#include "config.h"

#if USE_JEMALLOC

#include <Common/JemallocCacheAllocator.h>
#include <Common/JemallocCacheArena.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>

#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <cstring>

static constexpr size_t MALLOC_MIN_ALIGNMENT = alignof(std::max_align_t);

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}
}

namespace
{

void checkSize(size_t size)
{
    if (unlikely(size >= 0x8000000000000000ULL))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

int arenaFlags(size_t alignment)
{
    int flags = MALLOCX_ARENA(DB::JemallocCacheArena::getArenaIndex()) | MALLOCX_TCACHE_NONE;
    if (alignment > MALLOC_MIN_ALIGNMENT)
        flags |= MALLOCX_ALIGN(alignment);
    return flags;
}

int freeFlags()
{
    return MALLOCX_TCACHE_NONE;
}

}


void * JemallocCacheAllocator::alloc(size_t size, size_t alignment)
{
    checkSize(size);
    auto trace = CurrentMemoryTracker::alloc(size);

    void * ptr = je_mallocx(size, arenaFlags(alignment));
    if (unlikely(!ptr))
    {
        [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(size);
        throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocCacheAllocator: Cannot mallocx {}.", ReadableSize(size));
    }

    trace.onAlloc(ptr, size);
    return ptr;
}

void JemallocCacheAllocator::free(void * buf, size_t size)
{
    try
    {
        checkSize(size);
        je_sdallocx(buf, size, freeFlags());
        auto trace = CurrentMemoryTracker::free(size);
        trace.onFree(buf, size);
    }
    catch (...)
    {
        DB::tryLogCurrentException("JemallocCacheAllocator::free");
        throw;
    }
}

void * JemallocCacheAllocator::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    checkSize(new_size);

    if (old_size == new_size)
        return buf;

    auto trace_alloc = CurrentMemoryTracker::alloc(new_size);

    void * new_buf = je_rallocx(buf, new_size, arenaFlags(alignment));
    if (unlikely(!new_buf))
    {
        [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(new_size);
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
            "JemallocCacheAllocator: Cannot rallocx from {} to {}",
            ReadableSize(old_size),
            ReadableSize(new_size));
    }

    auto trace_free = CurrentMemoryTracker::free(old_size);
    trace_free.onFree(buf, old_size);
    trace_alloc.onAlloc(new_buf, new_size);

    return new_buf;
}

#endif
