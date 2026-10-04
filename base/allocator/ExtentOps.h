#pragma once

/// The page-level allocator: chooses the addresses that allocations requested by other modules will inhabit, and
/// updates the global metadata to reflect allocation/deallocation/purging decisions.
/// jemalloc: `extent.h`, `src/extent.c`.
///
/// Locking (ranks ascending): `pac->grow_mtx` (EXTENT_GROW) -> `ecache->mtx` (EXTENTS) -> `ExtentPool` (EDATA_CACHE) ->
/// rtree (RTREE) -> base (BASE). The rtree state of an extent is the authority for neighbor acquisition: a thread
/// holding the cache lock of state S may move any extent whose rtree state is S to `merging` (or `active`); extents in
/// active/merging state are owned by exactly one thread (see the neighbor rules in ExtentMap.h).
///
/// Functions that mirror jemalloc's `bool` functions return true on error.

#include <allocator/Common.h>
#include <allocator/Extent.h>

namespace jemalloc
{

class ExtentCache;
class ExtentHooks;
class PageAllocator;
class ThreadState;

/// jemalloc: LG_EXTENT_MAX_ACTIVE_FIT_DEFAULT (the option is `opt.lg_extent_max_active_fit`)
inline constexpr size_t LG_EXTENT_MAX_ACTIVE_FIT_DEFAULT = 6;

/// jemalloc: PROCESS_MADVISE_MAX_BATCH_DEFAULT (`JEMALLOC_HAVE_PROCESS_MADVISE` is not configured on any platform)
inline constexpr size_t PROCESS_MADVISE_MAX_BATCH_DEFAULT = 0;

/// Recycles an extent from `ecache` (never maps new memory). `expand_edata` is non-null for in-place expansion (then
/// only the extent directly after it is considered).
/// jemalloc: ecache_alloc
Extent * ecacheAlloc(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded);

/// Like `ecacheAlloc` on the retained cache, but grows it (or maps new memory) if needed.
/// jemalloc: ecache_alloc_grow
Extent * ecacheAllocGrow(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    ExtentCache * ecache,
    Extent * expand_edata,
    size_t size,
    size_t alignment,
    bool zero,
    bool guarded);

/// jemalloc: ecache_dalloc
void ecacheDalloc(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata);

/// Takes the LRU extent out of `ecache` (coalescing it first if coalescing is delayed), unless the cache has no more
/// than `npages_min` pages. The returned extent is active (dirty/muzzy) or deregistered (retained).
/// jemalloc: ecache_evict
Extent * ecacheEvict(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, size_t npages_min);

/// jemalloc: extent_gdump_add
void extentGdumpAdd(ThreadState * tsdn, const Extent * edata);

/// Does the metadata management portions of putting an unused extent into the given cache (coalesces and inserts
/// into the set).
/// jemalloc: extent_record
void extentRecord(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, ExtentCache * ecache, Extent * edata);

/// Maps a new extent from the OS (through the hooks). `*commit` is in/out.
/// jemalloc: extent_alloc_wrapper
Extent * extentAllocWrapper(
    ThreadState * tsdn,
    PageAllocator * pac,
    ExtentHooks * ehooks,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool zero,
    bool * commit,
    bool growing_retained);

/// Unmaps the extent, or (with `retain`) decommits/purges it and records it into the retained cache.
/// jemalloc: extent_dalloc_wrapper
void extentDallocWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata);

/// Only for the `process_madvise` path (not configured on any platform).
/// jemalloc: extent_dalloc_wrapper_purged
void extentDallocWrapperPurged(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata);

/// Destroys (unmaps) an already deregistered or acquired extent.
/// jemalloc: extent_destroy_wrapper
void extentDestroyWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata);

/// jemalloc: extent_commit_wrapper
bool extentCommitWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length);

/// jemalloc: extent_purge_lazy_wrapper
bool extentPurgeLazyWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length);

/// jemalloc: extent_purge_forced_wrapper
bool extentPurgeForcedWrapper(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, size_t offset, size_t length);

/// Splits `edata` into a lead of `size_a` (`edata` itself) and a trail of `size_b` (returned; null on error).
/// jemalloc: extent_split_wrapper
Extent * extentSplitWrapper(
    ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * edata, size_t size_a, size_t size_b, bool holding_core_locks);

/// Merges `b` (higher addresses) into `a`. Returns true on error.
/// jemalloc: extent_merge_wrapper
bool extentMergeWrapper(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, Extent * a, Extent * b);

/// Commits (if `commit`) and zeroes (if `zero`) the extent as needed. Returns true on error.
/// jemalloc: extent_commit_zero
bool extentCommitZero(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, bool commit, bool zero, bool growing_retained);

/// jemalloc: extent_sn_next
size_t extentSnNext(PageAllocator * pac);

/// Returns true on error.
/// jemalloc: extent_boot
bool extentBoot();

/// The gdump counters (`curpages`, `highpages`), for tests and introspection.
size_t extentGdumpCurpages();
size_t extentGdumpHighpages();

}
