#include <Common/JemallocNoDumpArenas.h>

#include "config.h"

#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/memory.h>

#if USE_JEMALLOC && defined(OS_LINUX)
#include <jemalloc/jemalloc.h>
#include <sys/mman.h>

#include <Common/PerCPU.h>

#include <optional>
#include <vector>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

}

namespace DB
{

#if USE_JEMALLOC && defined(OS_LINUX)

namespace
{

extent_hooks_t * default_hooks = nullptr;
extent_hooks_t no_dump_hooks;

void * allocNoDumpExtent(extent_hooks_t *, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_index)
{
    void * result = default_hooks->alloc(default_hooks, new_addr, size, alignment, zero, commit, arena_index);
    if (result && madvise(result, size, MADV_DONTDUMP) != 0)
    {
        default_hooks->destroy(default_hooks, result, size, *commit, arena_index);
        return nullptr;
    }
    return result;
}

bool commitNoDumpExtent(extent_hooks_t *, void * addr, size_t size, size_t offset, size_t length, unsigned arena_index)
{
    if (default_hooks->commit(default_hooks, addr, size, offset, length, arena_index))
        return true;
    return madvise(static_cast<char *>(addr) + offset, length, MADV_DONTDUMP) != 0;
}

std::vector<unsigned> createArenas()
{
    size_t hooks_size = sizeof(default_hooks);
    if (int err = je_mallctl("arena.0.extent_hooks", &default_hooks, &hooks_size, nullptr, 0))
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocNoDumpArenas: Failed to read default extent hooks, error: {}", err);

    no_dump_hooks = *default_hooks;
    no_dump_hooks.alloc = allocNoDumpExtent;
    no_dump_hooks.commit = commitNoDumpExtent;
    extent_hooks_t * hooks = &no_dump_hooks;

    std::vector<unsigned> indices(PerCPU::getNumCPUs());
    for (unsigned & index : indices)
    {
        size_t index_size = sizeof(index);
        if (int err = je_mallctl("arenas.create", &index, &index_size, &hooks, sizeof(hooks)))
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocNoDumpArenas: Failed to create jemalloc arena, error: {}", err);
    }
    return indices;
}

thread_local constinit int alloc_flags = 0;
thread_local constinit int dealloc_flags = MALLOCX_TCACHE_NONE;

struct ThreadCache
{
    int arena_flags;
    std::optional<unsigned> index;

    explicit ThreadCache(int arena_flags_) : arena_flags(arena_flags_)
    {
        unsigned created_index;
        size_t index_size = sizeof(created_index);
        if (je_mallctl("tcache.create", &created_index, &index_size, nullptr, 0) == 0)
            index = created_index;
        dealloc_flags = index ? MALLOCX_TCACHE(*index) : MALLOCX_TCACHE_NONE;
        alloc_flags = arena_flags | dealloc_flags;
    }

    ~ThreadCache()
    {
        if (index)
            je_mallctl("tcache.destroy", nullptr, nullptr, &*index, sizeof(*index));
        dealloc_flags = MALLOCX_TCACHE_NONE;
        alloc_flags = arena_flags | dealloc_flags;
    }
};

}

#endif

void * JemallocNoDumpArenas::allocate(size_t bytes, size_t alignment)
{
    const bool aligned = alignment > alignof(std::max_align_t);

#if USE_JEMALLOC && defined(OS_LINUX)
    if (alloc_flags == 0) [[unlikely]]
    {
        static const auto * arenas = new std::vector<unsigned>(createArenas());
        Int32 cpu = PerCPU::getCurrentCPU();
        static thread_local ThreadCache tcache(MALLOCX_ARENA((*arenas)[(cpu < 0 ? 0 : cpu) % arenas->size()]));
    }
    int flags = alloc_flags;
    if (aligned)
        flags |= MALLOCX_ALIGN(alignment);
#endif

    AllocationTrace trace;
    size_t actual_size = aligned ? Memory::trackMemory(bytes, trace, std::align_val_t(alignment)) : Memory::trackMemory(bytes, trace);

#if USE_JEMALLOC && defined(OS_LINUX)
    void * ptr = je_mallocx(bytes, flags);
#else
    void * ptr = aligned ? Memory::newNoExcept(bytes, std::align_val_t(alignment)) : Memory::newNoExcept(bytes);
#endif

    if (!ptr) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(actual_size);
        throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocNoDumpArenas: Cannot allocate {}.", ReadableSize(bytes));
    }

    trace.onAlloc(ptr, actual_size);
    return ptr;
}

void JemallocNoDumpArenas::deallocate(void * ptr) noexcept
{
    if (!ptr)
        return;

    AllocationTrace trace;
    size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);

#if USE_JEMALLOC && defined(OS_LINUX)
    je_dallocx(ptr, dealloc_flags);
#else
    Memory::deleteImpl(ptr);
#endif
}

}
