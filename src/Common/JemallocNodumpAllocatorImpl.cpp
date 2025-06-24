#ifdef USE_JEMALLOC
#include <Poco/Format.h>
#include <Common/JemallocNodumpAllocatorImpl.h>
#include <base/errnoToString.h>
#include <base/defines.h>
#include <sys/mman.h>
#include <cstring>

namespace DB
{

JemallocNodumpAllocatorImpl & JemallocNodumpAllocatorImpl::instance()
{
    static JemallocNodumpAllocatorImpl allocator;
    return allocator;
}

void * JemallocNodumpAllocatorImpl::allocate(size_t size, size_t alignment) const
{
    return mallocx(size, alignment ? flags | MALLOCX_ALIGN(alignment) : flags);
}

void JemallocNodumpAllocatorImpl::deallocate(void * p) const
{
    dallocx(p, flags);
}

void * JemallocNodumpAllocatorImpl::alloc(
    extent_hooks_t * extent, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind)
{
    void * result = original_alloc_(extent, new_addr, size, alignment, zero, commit, arena_ind);
    if (result != nullptr)
    {
        if (auto ret = madvise(result, size, MADV_DONTDUMP))
        {
            throw std::runtime_error(Poco::format("Failed to run madvise: %s", errnoToString(ret)));
        }
    }
    return result;
}

JemallocNodumpAllocatorImpl::JemallocNodumpAllocatorImpl()
{
    setupArena();
}

void JemallocNodumpAllocatorImpl::setupArena()
{
    size_t len = sizeof(arena_index);
    if (auto ret = mallctl("arenas.create", &arena_index, &len, nullptr, 0))
    {
        throw std::runtime_error(Poco::format("Failed to create jemalloc arena: %s", errnoToString(ret)));
    }
    flags = MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE;

    const std::string key = Poco::format("arena.%u.extent_hooks", arena_index);
    extent_hooks_t * hooks;
    len = sizeof(extent_hooks_t *);
    if (auto ret = mallctl(key.c_str(), &hooks, &len, nullptr, 0))
    {
        throw std::runtime_error(Poco::format("Failed to get default extent hooks (arena %u): %s", arena_index, errnoToString(ret)));
    }
    chassert(original_alloc_ == nullptr);
    original_alloc_ = hooks->alloc;

    extent_hooks_ = *hooks;
    extent_hooks_.alloc = &JemallocNodumpAllocatorImpl::alloc;
    extent_hooks_t * new_hooks = &extent_hooks_;
    if (auto ret = mallctl(key.c_str(), nullptr, nullptr, &new_hooks, sizeof(extent_hooks_t *)))
    {
        throw std::runtime_error(Poco::format("Failed to set custom extent hooks (arena %u): %s", arena_index, errnoToString(ret)));
    }
}

}
#endif
