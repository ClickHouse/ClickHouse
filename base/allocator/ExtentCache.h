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
    bool init(ThreadState * thread_state, ExtentState state_, unsigned idx_, bool delay_coalesce_);

    /// jemalloc: ecache_npages_get
    ALLOCATOR_ALWAYS_INLINE size_t numPagesGet() const { return extent_set.numPagesGet() + guarded_extent_set.numPagesGet(); }

    /// The number of extents in the given page size index.
    /// jemalloc: ecache_nextents_get
    ALLOCATOR_ALWAYS_INLINE size_t numExtentsGet(PageSizeClassIdx page_size_class_idx) const
    {
        return extent_set.numExtentsGet(page_size_class_idx) + guarded_extent_set.numExtentsGet(page_size_class_idx);
    }

    /// The sum total bytes of the extents in the given page size index.
    /// jemalloc: ecache_nbytes_get
    ALLOCATOR_ALWAYS_INLINE size_t numBytesGet(PageSizeClassIdx page_size_class_idx) const
    {
        return extent_set.numBytesGet(page_size_class_idx) + guarded_extent_set.numBytesGet(page_size_class_idx);
    }

    /// jemalloc: ecache_ind_get
    ALLOCATOR_ALWAYS_INLINE unsigned idxGet() const { return idx; }

    /// jemalloc: ecache_prefork
    void prefork(ThreadState * thread_state) { mutex.prefork(thread_state); }
    /// jemalloc: ecache_postfork_parent
    void postforkParent(ThreadState * thread_state) { mutex.postforkParent(thread_state); }
    /// jemalloc: ecache_postfork_child
    void postforkChild(ThreadState * thread_state) { mutex.postforkChild(thread_state); }

    /// "extents", `MutexRank::EXTENTS`.
    Mutex mutex;
    /// Non-guarded extents.
    ExtentSet extent_set;
    /// Guarded extents (`sanitizer_guard_small` / `sanitizer_guard_large`).
    ExtentSet guarded_extent_set;
    /// All stored extents must be in the same state.
    ExtentState state = extent_state_active;
    /// The index of the extent hooks the cache is associated with (the arena index).
    unsigned idx = 0;
    /// If true, delay coalescing until eviction; otherwise coalesce during deallocation.
    bool delay_coalesce = false;
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(ExtentCache) == (LOG2_PAGE == 12 ? 19448 : (LOG2_PAGE == 14 ? 18664 : 17896)), "ecache_t size (aarch64 glibc)");
#elif defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
static_assert(LOG2_PAGE != 12 || sizeof(ExtentCache) == 19440, "ecache_t size (x86_64 glibc)");
#endif

}
