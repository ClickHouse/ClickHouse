#include <Common/IMemoryReleasableCache.h>

#include <atomic>

namespace DB
{

namespace
{
    std::atomic<IMemoryReleasableCache *> memory_releasable_cache{nullptr};
}

void setMemoryReleasableCache(IMemoryReleasableCache * cache)
{
    memory_releasable_cache.store(cache);
}

IMemoryReleasableCache * getMemoryReleasableCache()
{
    return memory_releasable_cache.load(std::memory_order_relaxed);
}

}
