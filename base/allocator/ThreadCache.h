#pragma once

/// The thread cache (jemalloc: `tcache.c`, `tcache_inlines.h`, `tcache_externs.h`, `tcache_types.h`, and the tcache
/// accessors of `jemalloc_internal_inlines_a.h`). The data layout is in ThreadCacheData.h.
///
/// The automatic tcache of a thread is embedded in its `ThreadState` (`ThreadState::thread_cache` is the last field of the
/// fast data, `ThreadState::thread_cache_slow` lives in the slow data). Explicit tcaches (`tcache.create`, `MALLOCX_TCACHE`)
/// are one internal allocation `[stacks][ThreadCache][ThreadCacheSlow]` registered in the `explicit_thread_caches` array.
///
/// The fill/flush code (`arena_ptr_array_fill_small`, `arena_ptr_array_flush`) is owned by the arena module.
///
/// Under ClickHouse's configuration the active GC is the time-gated, locality-aware one
/// (`opt.experimental_tcache_gc` = true); the legacy one-bin-per-event GC is kept behind the option.

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/CacheBin.h>
#include <allocator/Common.h>
#include <allocator/Format.h>
#include <allocator/Options.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCacheData.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <cstdlib>
#include <cstring>

namespace jemalloc
{

class Base;

/// Number of tcache bins: `SIZE_CLASS_NUM_BINS` small-object bins, plus 0 or more large-object bins. This is only used during
/// thread initialization; changing it does not affect initialized threads. To change the number of tcache bins in
/// use, refer to `thread_cache_num_bins` of each tcache.
/// jemalloc: global_do_not_change_tcache_nbins (`arenas.nhbins`)
extern constinit unsigned global_do_not_change_thread_cache_num_bins;

/// Maximum cached size class. Same as above: only used during thread initialization.
/// jemalloc: global_do_not_change_tcache_maxclass (`arenas.tcache_max`)
extern constinit size_t global_do_not_change_thread_cache_max_class;

/// Explicit tcaches, managed via the `thread_cache.{create,flush,destroy}` mallctls and usable via the `MALLOCX_TCACHE()`
/// flag. Allocated (`MALLOCX_THREAD_CACHE_MAX + 1` slots from the base) the first time an explicit tcache is created.
/// jemalloc: tcaches
extern constinit ThreadCaches * explicit_thread_caches;

/// --- Accessors (jemalloc_internal_inlines_a.h, tcache_inlines.h) --------------------------------------------------

/// jemalloc: tcache_assert_initialized
void threadCacheAssertInitialized(ThreadCache * thread_cache);

/// The thread specific auto tcache might be unavailable if: 1) during tcache initialization, or 2) disabled through
/// `thread.tcache.enabled` or the options. This check covers all cases.
/// jemalloc: tcache_available
ALLOCATOR_ALWAYS_INLINE bool threadCacheAvailable(ThreadState & thread_state)
{
    if (ALLOCATOR_LIKELY(thread_state.thread_cache_enabled))
    {
        /// Associated arena == null implies tcache init in progress.
        if constexpr (config::debug)
        {
            if (thread_state.threadCacheSlowGet()->arena != nullptr)
                threadCacheAssertInitialized(thread_state.threadCacheGet());
        }
        return true;
    }
    return false;
}

/// jemalloc: tcache_get
ALLOCATOR_ALWAYS_INLINE ThreadCache * threadCacheGet(ThreadState & thread_state)
{
    if (!threadCacheAvailable(thread_state))
        return nullptr;
    return thread_state.threadCacheGet();
}

/// jemalloc: tcache_slow_get
ALLOCATOR_ALWAYS_INLINE ThreadCacheSlow * threadCacheSlowGet(ThreadState & thread_state)
{
    if (!threadCacheAvailable(thread_state))
        return nullptr;
    return thread_state.threadCacheSlowGet();
}

/// jemalloc: tcache_enabled_get
ALLOCATOR_ALWAYS_INLINE bool threadCacheEnabledGet(ThreadState & thread_state)
{
    return thread_state.thread_cache_enabled;
}

/// jemalloc: tcache_nbins_get
ALLOCATOR_ALWAYS_INLINE unsigned threadCacheNumBinsGet(const ThreadCacheSlow * thread_cache_slow)
{
    ALLOCATOR_ASSERT(thread_cache_slow != nullptr);
    unsigned num_bins = thread_cache_slow->thread_cache_num_bins;
    ALLOCATOR_ASSERT(num_bins <= THREAD_CACHE_NUM_BINS_MAX);
    return num_bins;
}

/// jemalloc: tcache_max_get
ALLOCATOR_ALWAYS_INLINE size_t threadCacheMaxGet(const ThreadCacheSlow * thread_cache_slow)
{
    ALLOCATOR_ASSERT(thread_cache_slow != nullptr);
    size_t thread_cache_max = size_classes::indexToSize(threadCacheNumBinsGet(thread_cache_slow) - 1);
    ALLOCATOR_ASSERT(thread_cache_max <= THREAD_CACHE_MAX_CLASS_LIMIT);
    return thread_cache_max;
}

/// jemalloc: tcache_max_set
ALLOCATOR_ALWAYS_INLINE void threadCacheMaxSet(ThreadCacheSlow * thread_cache_slow, size_t thread_cache_max)
{
    ALLOCATOR_ASSERT(thread_cache_slow != nullptr);
    ALLOCATOR_ASSERT(thread_cache_max <= THREAD_CACHE_MAX_CLASS_LIMIT);
    thread_cache_slow->thread_cache_num_bins = size_classes::sizeToIndex(thread_cache_max) + 1;
}

/// jemalloc: tcache_bin_settings_backup
ALLOCATOR_ALWAYS_INLINE void threadCacheBinSettingsBackup(const ThreadCache * thread_cache, CacheBinInfo * thread_cache_bin_info)
{
    for (unsigned i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
        thread_cache_bin_info[i].init(thread_cache->bins[i].numCachedMaxGetUnsafe());
}

/// If a bin's ind >= nbins or ncached_max == 0, it must be disabled. If a bin is enabled, it has ind < nbins and
/// ncached_max > 0. In release builds this is just the `stack_head` compare.
/// jemalloc: tcache_bin_disabled
ALLOCATOR_ALWAYS_INLINE bool
threadCacheBinDisabled(SizeClassIdx idx, const CacheBin * bin, [[maybe_unused]] const ThreadCacheSlow * thread_cache_slow)
{
    ALLOCATOR_ASSERT(bin != nullptr);
    ALLOCATOR_ASSERT(idx < THREAD_CACHE_NUM_BINS_MAX);
    bool disabled = bin->disabled();

    if constexpr (config::debug)
    {
        unsigned num_bins = threadCacheNumBinsGet(thread_cache_slow);
        CacheBinSize num_cached_max = bin->numCachedMaxGetUnsafe();
        if (idx >= num_bins)
            ALLOCATOR_ASSERT(disabled);
        else
            ALLOCATOR_ASSERT(!disabled || num_cached_max == 0);
        if (num_cached_max == 0)
            ALLOCATOR_ASSERT(disabled);
        else
            ALLOCATOR_ASSERT(!disabled || idx >= num_bins);
        if (disabled)
            ALLOCATOR_ASSERT(idx >= num_bins || num_cached_max == 0);
        else
            ALLOCATOR_ASSERT(idx < num_bins && num_cached_max > 0);
    }

    return disabled;
}

/// --- Slow paths (ThreadCache.cpp) ----------------------------------------------------------------------------------

/// jemalloc: tcache_salloc
size_t threadCacheAllocationSize(ThreadState * thread_state, const void * ptr);

/// Fills the (empty) bin from the arena and allocates from it.
/// jemalloc: tcache_alloc_small_hard
void * threadCacheAllocSmallHard(
    ThreadState * thread_state,
    Arena * arena,
    ThreadCache * thread_cache,
    CacheBin * cache_bin,
    SizeClassIdx bin_idx,
    bool & thread_cache_success);

/// Flushes the bin down to `remainder` cached items (the bottom items are flushed).
/// jemalloc: tcache_bin_flush_small, tcache_bin_flush_large
void threadCacheBinFlushSmall(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, unsigned remainder);
void threadCacheBinFlushLarge(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, unsigned remainder);

/// Flushes the stashed (UAF detection) items, after checking their junk. A no-op when nothing is stashed.
/// jemalloc: tcache_bin_flush_stashed
void threadCacheBinFlushStashed(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, bool is_small);

/// --- Fast paths (tcache_inlines.h) ---------------------------------------------------------------------------------

/// jemalloc: tcache_alloc_small
ALLOCATOR_ALWAYS_INLINE void * threadCacheAllocSmall(
    ThreadState & thread_state, Arena * arena, ThreadCache * thread_cache, size_t size, SizeClassIdx bin_idx, bool zero, bool /*slow_path*/)
{
    void * result;
    bool thread_cache_success;

    ALLOCATOR_ASSERT(bin_idx < SIZE_CLASS_NUM_BINS);
    CacheBin * bin = &thread_cache->bins[bin_idx];
    result = bin->alloc(thread_cache_success);
    ALLOCATOR_ASSERT(thread_cache_success == (result != nullptr));
    if (ALLOCATOR_UNLIKELY(!thread_cache_success))
    {
        bool thread_cache_hard_success;
        arena = arenaChoose(thread_state, arena);
        if (ALLOCATOR_UNLIKELY(arena == nullptr))
            return nullptr;
        if (ALLOCATOR_UNLIKELY(threadCacheBinDisabled(bin_idx, bin, thread_cache->thread_cache_slow)))
        {
            /// Stats and zero are handled directly by the arena.
            return arenaMallocHard(&thread_state, arena, size, bin_idx, zero, /* slab */ true);
        }
        threadCacheBinFlushStashed(thread_state, thread_cache, bin, bin_idx, /* is_small */ true);

        result = threadCacheAllocSmallHard(&thread_state, arena, thread_cache, bin, bin_idx, thread_cache_hard_success);
        if (!thread_cache_hard_success)
            return nullptr;
    }

    ALLOCATOR_ASSERT(result);
    if (ALLOCATOR_UNLIKELY(zero))
    {
        size_t usable_size = size_classes::indexToSize(bin_idx);
        ALLOCATOR_ASSERT(threadCacheAllocationSize(&thread_state, result) == usable_size);
        memset(result, 0, usable_size);
    }
    if constexpr (config::stats)
        ++bin->thread_cache_stats.num_requests;
    return result;
}

/// jemalloc: tcache_alloc_large
ALLOCATOR_ALWAYS_INLINE void * threadCacheAllocLarge(
    ThreadState & thread_state, Arena * arena, ThreadCache * thread_cache, size_t size, SizeClassIdx bin_idx, bool zero, bool /*slow_path*/)
{
    void * result;
    bool thread_cache_success;

    CacheBin * bin = &thread_cache->bins[bin_idx];
    ALLOCATOR_ASSERT(bin_idx >= SIZE_CLASS_NUM_BINS && !threadCacheBinDisabled(bin_idx, bin, thread_cache->thread_cache_slow));
    result = bin->alloc(thread_cache_success);
    ALLOCATOR_ASSERT(thread_cache_success == (result != nullptr));
    if (ALLOCATOR_UNLIKELY(!thread_cache_success))
    {
        /// Only allocate one large object at a time, because it's quite expensive to create one and not use it.
        arena = arenaChoose(thread_state, arena);
        if (ALLOCATOR_UNLIKELY(arena == nullptr))
            return nullptr;
        threadCacheBinFlushStashed(thread_state, thread_cache, bin, bin_idx, /* is_small */ false);

        result = largeMalloc(&thread_state, arena, size_classes::sizeToUsableSize(size), zero);
        if (result == nullptr)
            return nullptr;
    }
    else
    {
        if (ALLOCATOR_UNLIKELY(zero))
        {
            size_t usable_size = size_classes::indexToSize(bin_idx);
            ALLOCATOR_ASSERT(usable_size <= threadCacheMaxGet(thread_cache->thread_cache_slow));
            memset(result, 0, usable_size);
        }

        if constexpr (config::stats)
            ++bin->thread_cache_stats.num_requests;
    }

    return result;
}

/// jemalloc: tcache_dalloc_small
ALLOCATOR_ALWAYS_INLINE void
threadCacheDeallocateSmall(ThreadState & thread_state, ThreadCache * thread_cache, void * ptr, SizeClassIdx bin_idx, bool /*slow_path*/)
{
    ALLOCATOR_ASSERT(threadCacheAllocationSize(&thread_state, ptr) <= SIZE_CLASS_SMALL_MAX_CLASS);

    CacheBin * bin = &thread_cache->bins[bin_idx];
    /// Not marking the branch unlikely because this is past the free fast path (which handles the most common cases),
    /// i.e. at this point it's often uncommon cases.
    if (cacheBinNonFastAligned(ptr))
    {
        /// Junk unconditionally, even if bin is full.
        sanitizerJunkPtr(ptr, size_classes::indexToSize(bin_idx));
        if (bin->stash(ptr))
            return;
        ALLOCATOR_ASSERT(bin->full());
        /// Bin full; fall through into the flush branch.
    }

    if (ALLOCATOR_UNLIKELY(!bin->deallocateEasy(ptr)))
    {
        if (ALLOCATOR_UNLIKELY(threadCacheBinDisabled(bin_idx, bin, thread_cache->thread_cache_slow)))
        {
            arenaDeallocateSmall(&thread_state, ptr);
            return;
        }
        CacheBinSize max = bin->numCachedMaxGet();
        unsigned remain = max >> options.log2_thread_cache_flush_small_division;
        threadCacheBinFlushSmall(thread_state, thread_cache, bin, bin_idx, remain);
        [[maybe_unused]] bool result = bin->deallocateEasy(ptr);
        ALLOCATOR_ASSERT(result);
    }
}

/// jemalloc: tcache_dalloc_large
ALLOCATOR_ALWAYS_INLINE void
threadCacheDeallocateLarge(ThreadState & thread_state, ThreadCache * thread_cache, void * ptr, SizeClassIdx bin_idx, bool /*slow_path*/)
{
    ALLOCATOR_ASSERT(threadCacheAllocationSize(&thread_state, ptr) > SIZE_CLASS_SMALL_MAX_CLASS);
    ALLOCATOR_ASSERT(threadCacheAllocationSize(&thread_state, ptr) <= threadCacheMaxGet(thread_cache->thread_cache_slow));
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(bin_idx, &thread_cache->bins[bin_idx], thread_cache->thread_cache_slow));

    CacheBin * bin = &thread_cache->bins[bin_idx];
    if (ALLOCATOR_UNLIKELY(!bin->deallocateEasy(ptr)))
    {
        unsigned remain = bin->numCachedMaxGet() >> options.log2_thread_cache_flush_large_division;
        threadCacheBinFlushLarge(thread_state, thread_cache, bin, bin_idx, remain);
        [[maybe_unused]] bool result = bin->deallocateEasy(ptr);
        ALLOCATOR_ASSERT(result);
    }
}

/// Creates an explicit tcache (for `tcache.create`, and the re-creation of a flushed one). Returns null on OOM.
/// jemalloc: tcache_create_explicit
ThreadCache * threadCacheCreateExplicit(ThreadState & thread_state);

/// jemalloc: tcaches_get
ALLOCATOR_ALWAYS_INLINE ThreadCache * explicitThreadCachesGet(ThreadState & thread_state, unsigned idx)
{
    ThreadCaches * element = &explicit_thread_caches[idx];
    if (ALLOCATOR_UNLIKELY(element->thread_cache == nullptr))
    {
        printMessage("<jemalloc>: invalid tcache id (%u).\n", idx);
        abort();
    }
    else if (ALLOCATOR_UNLIKELY(element->thread_cache == EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT))
    {
        /// On out of memory, proceed without a tcache and keep the element marked for reinitialization, so that the
        /// id stays valid. jemalloc stores the null tcache, which makes the id invalid.
        ThreadCache * thread_cache = threadCacheCreateExplicit(thread_state);
        if (ALLOCATOR_UNLIKELY(thread_cache == nullptr))
            return nullptr;
        element->thread_cache = thread_cache;
    }
    return element->thread_cache;
}

/// --- Settings, life cycle (ThreadCache.cpp) ------------------------------------------------------------------------

/// The default `num_cached_max` of every bin: computed by `threadCacheBoot` (from `opt.tcache_ncached_max` where set,
/// `threadCacheNumCachedMaxCompute` otherwise); not modified afterwards.
/// jemalloc: tcache_get_default_ncached_max (`opt_tcache_ncached_max` after `tcache_boot`)
const CacheBinInfo * threadCacheGetDefaultNumCachedMax();

/// Whether `thread_cache_num_cached_max` (malloc_conf) set the bin.
/// jemalloc: tcache_get_default_ncached_max_set
bool threadCacheGetDefaultNumCachedMaxSet(SizeClassIdx idx);

/// The default `num_cached_max` of a bin computed from the slab size and the `tcache_nslots_*` options.
/// jemalloc: tcache_ncached_max_compute
unsigned threadCacheNumCachedMaxCompute(SizeClassIdx size_class_idx);

/// Computes the values for each bin (bins with indices >= tcache_nbins cache nothing, but get a value too).
/// jemalloc: tcache_bin_info_compute
void threadCacheBinInfoCompute(CacheBinInfo * thread_cache_bin_info);

/// `thread.tcache.ncached_max.read_sizeclass`. Returns true on error (size > `THREAD_CACHE_MAX_CLASS_LIMIT`).
/// jemalloc: tcache_bin_ncached_max_read
bool threadCacheBinNumCachedMaxRead(ThreadState & thread_state, size_t bin_size, CacheBinSize & num_cached_max);

/// `thread.tcache.ncached_max.write`: parses the settings over the current ones and reboots the tcache.
/// Returns true on error. The tcache must be available.
/// jemalloc: tcache_bins_ncached_max_write
bool threadCacheBinsNumCachedMaxWrite(ThreadState & thread_state, const char * settings, size_t len);

/// jemalloc: tcache_arena_associate, tcache_arena_reassociate
void threadCacheArenaAssociate(ThreadState * thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, Arena * arena);
void threadCacheArenaReassociate(
    ThreadState * thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, Arena * arena);

/// `thread.tcache.max`. Returns true on error.
/// jemalloc: thread_tcache_max_set
bool threadThreadCacheMaxSet(ThreadState & thread_state, size_t thread_cache_max);

/// Destroys the automatic tcache of the thread (if available) and resets its bins to the zero state.
/// jemalloc: tcache_cleanup
void threadCacheCleanup(ThreadState & thread_state);

/// Merges and resets the tcache request counters into the arena stats.
/// jemalloc: tcache_stats_merge
void threadCacheStatsMerge(ThreadState * thread_state, ThreadCache * thread_cache, Arena * arena);

/// jemalloc: tcaches_create (returns true on error), tcaches_flush, tcaches_destroy
bool explicitThreadCachesCreate(ThreadState & thread_state, Base * base, unsigned & r_idx);
void explicitThreadCachesFlush(ThreadState & thread_state, unsigned idx);
void explicitThreadCachesDestroy(ThreadState & thread_state, unsigned idx);

/// Returns true on error.
/// jemalloc: tcache_boot
bool threadCacheBoot(ThreadState * thread_state, Base * base);

/// jemalloc: tcache_prefork, tcache_postfork_parent, tcache_postfork_child
void threadCachePrefork(ThreadState * thread_state);
void threadCachePostforkParent(ThreadState * thread_state);
void threadCachePostforkChild(ThreadState * thread_state);

/// Flushes every enabled bin of the automatic tcache (`thread.tcache.flush`, `thread.idle`). The tcache must be
/// available.
/// jemalloc: tcache_flush
void threadCacheFlush(ThreadState & thread_state);

/// `threadCacheThreadStateDataInit` (ThreadState.h) is jemalloc's `tsd_tcache_enabled_data_init`.

/// `thread.tcache.enabled`.
/// jemalloc: tcache_enabled_set
void threadCacheEnabledSet(ThreadState & thread_state, bool enabled);

/// The allocation of the cache bin stacks of the automatic tcache. A function pointer (as in jemalloc) so that tests
/// can inject failures.
/// jemalloc: tcache_stack_alloc
extern constinit void * (*thread_cache_stack_alloc)(ThreadState * thread_state, size_t size, size_t alignment);

/// --- Internals exposed for the tests ------------------------------------------------------------------------------

namespace thread_cache_detail
{

/// jemalloc: tcache_bin_fill_ctl_init, tcache_bin_fill_ctl_get
void threadCacheBinFillControlInit(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx);
CacheBinFillControl * threadCacheBinFillControlGet(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx);
/// jemalloc: tcache_nfill_small_lg_div_get
uint8_t threadCacheNumFillSmallLog2DivisionGet(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx);
/// jemalloc: tcache_nfill_small_burst_prepare, tcache_nfill_small_burst_reset
void threadCacheNumFillSmallBurstPrepare(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx);
void threadCacheNumFillSmallBurstReset(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx);
/// jemalloc: tcache_nfill_small_gc_update
void threadCacheNumFillSmallGCUpdate(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx, CacheBinSize limit);
/// jemalloc: tcache_gc_item_delay_compute
uint8_t threadCacheGCItemDelayCompute(SizeClassIdx size_class_idx);
/// jemalloc: tcache_gc_is_addr_remote
bool threadCacheGCIsAddrRemote(void * addr, uintptr_t min, uintptr_t max);
/// jemalloc: tcache_gc_small_nremote_get
CacheBinSize threadCacheGCSmallNumRemoteGet(
    CacheBin * cache_bin, void * addr, uintptr_t & addr_min, uintptr_t & addr_max, SizeClassIdx size_class_idx, size_t num_flush);
/// jemalloc: tcache_gc_small_bin_shuffle
void threadCacheGCSmallBinShuffle(CacheBin * cache_bin, CacheBinSize num_remote, uintptr_t addr_min, uintptr_t addr_max);

}

/// The GC event handler entry points (ThreadEvent.h): `threadCacheGCNewEventWait`, `threadCacheGCPostponedEventWait`,
/// `threadCacheGCEvent`.

}
