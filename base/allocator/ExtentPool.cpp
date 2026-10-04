#include <allocator/ExtentPool.h>

#include <allocator/Base.h>

namespace jemalloc
{

/// Measured from the C build (embedded in `pa_shard_t`).
static_assert(sizeof(ExtentPool) == 16 + 8 + sizeof(Mutex) + 8);

/// jemalloc: edata_cache_init
bool ExtentPool::init(Base * base_)
{
    avail.init();
    /// This is not strictly necessary, since the `ExtentPool` is only created inside an arena, which is zeroed on
    /// creation. But this is handy as a safety measure.
    count_.store(0, std::memory_order_relaxed);
    if (mtx.init("edata_cache", MutexRank::EDATA_CACHE, MutexLockOrder::RankExclusive))
        return true;
    base = base_;
    return false;
}

/// jemalloc: edata_cache_get
Extent * ExtentPool::get(ThreadState * tsdn)
{
    mtx.lock(tsdn);
    Extent * edata = avail.first();
    if (edata == nullptr)
    {
        mtx.unlock(tsdn);
        return base->allocExtent(tsdn);
    }
    avail.remove(edata);
    /// jemalloc: atomic_load_sub_store_zu (not an atomic RMW: a relaxed load and store under the mutex).
    count_.store(count_.load(std::memory_order_relaxed) - 1, std::memory_order_relaxed);
    mtx.unlock(tsdn);
    return edata;
}

/// jemalloc: edata_cache_put
void ExtentPool::put(ThreadState * tsdn, Extent * edata)
{
    mtx.lock(tsdn);
    avail.insert(edata);
    count_.store(count_.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    mtx.unlock(tsdn);
}

}
