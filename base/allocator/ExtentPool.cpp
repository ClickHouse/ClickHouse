#include <allocator/ExtentPool.h>

#include <allocator/Base.h>

namespace jemalloc
{

/// Measured from the C build (embedded in `pa_shard_t`).
static_assert(sizeof(ExtentPool) == 16 + 8 + sizeof(Mutex) + 8);

/// jemalloc: edata_cache_init
bool ExtentPool::init(Base * base_)
{
    available.init();
    /// This is not strictly necessary, since the `ExtentPool` is only created inside an arena, which is zeroed on
    /// creation. But this is handy as a safety measure.
    count_.store(0, std::memory_order_relaxed);
    if (mutex.init("edata_cache", MutexRank::EXTENT_POOL, MutexLockOrder::RankExclusive))
        return true;
    base = base_;
    return false;
}

/// jemalloc: edata_cache_get
Extent * ExtentPool::get(ThreadState * thread_state)
{
    mutex.lock(thread_state);
    Extent * extent = available.first();
    if (extent == nullptr)
    {
        mutex.unlock(thread_state);
        return base->allocExtent(thread_state);
    }
    available.remove(extent);
    /// jemalloc: atomic_load_sub_store_zu (not an atomic RMW: a relaxed load and store under the mutex).
    count_.store(count_.load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
    mutex.unlock(thread_state);
    return extent;
}

/// jemalloc: edata_cache_put
void ExtentPool::put(ThreadState * thread_state, Extent * extent)
{
    mutex.lock(thread_state);
    available.insert(extent);
    count_.store(count_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    mutex.unlock(thread_state);
}

}
