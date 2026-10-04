#pragma once

/// A cache of unused extents in one state (dirty, muzzy or retained) of a page allocator.
/// jemalloc: `ecache.h`, `src/ecache.c`.

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/ExtentSet.h>
#include <allocator/Mutex.h>

namespace jemalloc
{

class ThreadState;

/// jemalloc: ecache_t
class ExtentCache
{
public:
    constexpr ExtentCache() = default;

    ExtentCache(const ExtentCache &) = delete;
    ExtentCache & operator=(const ExtentCache &) = delete;

    /// Returns true on error.
    /// jemalloc: ecache_init
    bool init(ThreadState * tsdn, ExtentState state_, unsigned ind_, bool delay_coalesce_);

    /// jemalloc: ecache_npages_get
    JE_ALWAYS_INLINE size_t npagesGet() const { return eset.npagesGet() + guarded_eset.npagesGet(); }

    /// The number of extents in the given page size index.
    /// jemalloc: ecache_nextents_get
    JE_ALWAYS_INLINE size_t nextentsGet(pszind_t pind) const { return eset.nextentsGet(pind) + guarded_eset.nextentsGet(pind); }

    /// The sum total bytes of the extents in the given page size index.
    /// jemalloc: ecache_nbytes_get
    JE_ALWAYS_INLINE size_t nbytesGet(pszind_t pind) const { return eset.nbytesGet(pind) + guarded_eset.nbytesGet(pind); }

    /// jemalloc: ecache_ind_get
    JE_ALWAYS_INLINE unsigned indGet() const { return ind; }

    /// jemalloc: ecache_prefork
    void prefork(ThreadState * tsdn) { mtx.prefork(tsdn); }
    /// jemalloc: ecache_postfork_parent
    void postforkParent(ThreadState * tsdn) { mtx.postforkParent(tsdn); }
    /// jemalloc: ecache_postfork_child
    void postforkChild(ThreadState * tsdn) { mtx.postforkChild(tsdn); }

    /// "extents", `MutexRank::EXTENTS`.
    Mutex mtx;
    /// Non-guarded extents.
    ExtentSet eset;
    /// Guarded extents (`san_guard_small` / `san_guard_large`).
    ExtentSet guarded_eset;
    /// All stored extents must be in the same state.
    ExtentState state = extent_state_active;
    /// The index of the extent hooks the cache is associated with (the arena index).
    unsigned ind = 0;
    /// If true, delay coalescing until eviction; otherwise coalesce during deallocation.
    bool delay_coalesce = false;
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(ExtentCache) == (LG_PAGE == 12 ? 19448 : (LG_PAGE == 14 ? 18664 : 17896)), "ecache_t size (aarch64 glibc)");
#elif defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
static_assert(LG_PAGE != 12 || sizeof(ExtentCache) == 19440, "ecache_t size (x86_64 glibc)");
#endif

}
