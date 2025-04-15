#include <Poco/Format.h>
#include <Common/JemallocNodumpAllocator.h>
#include <base/errnoToString.h>
#include <base/defines.h>
#include <cstring>

namespace DB
{

JemallocNodumpAllocatorImpl & JemallocNodumpAllocatorImpl::instance()
{
    static JemallocNodumpAllocatorImpl allocator;
    return allocator;
}

void * JemallocNodumpAllocatorImpl::allocate(size_t size) const
{
    return mallocx(size, flags);
}

void * JemallocNodumpAllocatorImpl::reallocate(void * p, size_t size) const
{
    return rallocx(p, size, flags);
}

void JemallocNodumpAllocatorImpl::deallocate(void * p) const
{
    dallocx(p, flags);
}

void JemallocNodumpAllocatorImpl::deallocate(void * p, void * userData)
{
    const auto flags = reinterpret_cast<uint64_t>(userData);
    dallocx(p, static_cast<int>(flags));
}

void * JemallocNodumpAllocatorImpl::alloc(
    extent_hooks_t * extent, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind)
{
    void * result = original_alloc_(extent, new_addr, size, alignment, zero, commit, arena_ind);
    if (result != nullptr)
    {
        if (auto ret = madvise(result, size, MADV_DONTDUMP))
        {
            throw std::runtime_error(Poco::format("Failed to run madvise: {}", errnoToString(ret)));
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
        throw std::runtime_error(Poco::format("Failed to create jemalloc arena: {}", errnoToString(ret)));
    }
    flags = MALLOCX_ARENA(arena_index) | MALLOCX_TCACHE_NONE;

    const std::string key = Poco::format("arena.{}.extent_hooks", arena_index);
    extent_hooks_t * hooks;
    len = sizeof(extent_hooks_t *);
    if (auto ret = mallctl(key.c_str(), &hooks, &len, nullptr, 0))
    {
        throw std::runtime_error(Poco::format("Failed to get default extent hooks: {}", errnoToString(ret)));
    }
    chassert(original_alloc_ == nullptr);
    original_alloc_ = hooks->alloc;

    extent_hooks_ = *hooks;
    extent_hooks_.alloc = &JemallocNodumpAllocatorImpl::alloc;
    extent_hooks_t * new_hooks = &extent_hooks_;
    if (auto ret = mallctl(key.c_str(), nullptr, nullptr, &new_hooks, sizeof(extent_hooks_t *)))
    {
        throw std::runtime_error(Poco::format("Failed to set custom extent hooks: {}", errnoToString(ret)));
    }

    const std::string arena_name_key = Poco::format("arena.{}.name", arena_index);
    const char * arena_name_str = "JemallocNodumpAllocator";
    mallctl(arena_name_key.c_str(), nullptr, nullptr, &arena_name_str, sizeof(void *));
}

}
