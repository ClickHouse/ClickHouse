#pragma once

/// The thread cache (jemalloc: `tcache.c`, `tcache_inlines.h`, `tcache_externs.h`, `tcache_types.h`, and the tcache
/// accessors of `jemalloc_internal_inlines_a.h`). The data layout is in ThreadCacheData.h.
///
/// The automatic tcache of a thread is embedded in its `ThreadState` (`ThreadState::tcache` is the last field of the
/// fast data, `ThreadState::tcache_slow` lives in the slow data). Explicit tcaches (`tcache.create`, `MALLOCX_TCACHE`)
/// are one internal allocation `[stacks][ThreadCache][ThreadCacheSlow]` registered in the `tcaches` array.
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

/// Number of tcache bins: `SC_NBINS` small-object bins, plus 0 or more large-object bins. This is only used during
/// thread initialization; changing it does not affect initialized threads. To change the number of tcache bins in
/// use, refer to `tcache_nbins` of each tcache.
/// jemalloc: global_do_not_change_tcache_nbins (`arenas.nhbins`)
extern constinit unsigned global_do_not_change_tcache_nbins;

/// Maximum cached size class. Same as above: only used during thread initialization.
/// jemalloc: global_do_not_change_tcache_maxclass (`arenas.tcache_max`)
extern constinit size_t global_do_not_change_tcache_maxclass;

/// Explicit tcaches, managed via the `tcache.{create,flush,destroy}` mallctls and usable via the `MALLOCX_TCACHE()`
/// flag. Allocated (`MALLOCX_TCACHE_MAX + 1` slots from the base) the first time an explicit tcache is created.
/// jemalloc: tcaches
extern constinit ThreadCaches * tcaches;

/// --- Accessors (jemalloc_internal_inlines_a.h, tcache_inlines.h) --------------------------------------------------

/// jemalloc: tcache_assert_initialized
void tcacheAssertInitialized(ThreadCache * tcache);

/// The thread specific auto tcache might be unavailable if: 1) during tcache initialization, or 2) disabled through
/// `thread.tcache.enabled` or the options. This check covers all cases.
/// jemalloc: tcache_available
JE_ALWAYS_INLINE bool tcacheAvailable(ThreadState & tsd)
{
    if (JE_LIKELY(tsd.tcache_enabled))
    {
        /// Associated arena == null implies tcache init in progress.
        if constexpr (config::debug)
        {
            if (tsd.tcacheSlowGet()->arena != nullptr)
                tcacheAssertInitialized(tsd.tcacheGet());
        }
        return true;
    }
    return false;
}

/// jemalloc: tcache_get
JE_ALWAYS_INLINE ThreadCache * tcacheGet(ThreadState & tsd)
{
    if (!tcacheAvailable(tsd))
        return nullptr;
    return tsd.tcacheGet();
}

/// jemalloc: tcache_slow_get
JE_ALWAYS_INLINE ThreadCacheSlow * tcacheSlowGet(ThreadState & tsd)
{
    if (!tcacheAvailable(tsd))
        return nullptr;
    return tsd.tcacheSlowGet();
}

/// jemalloc: tcache_enabled_get
JE_ALWAYS_INLINE bool tcacheEnabledGet(ThreadState & tsd)
{
    return tsd.tcache_enabled;
}

/// jemalloc: tcache_nbins_get
JE_ALWAYS_INLINE unsigned tcacheNbinsGet(const ThreadCacheSlow * tcache_slow)
{
    JE_ASSERT(tcache_slow != nullptr);
    unsigned nbins = tcache_slow->tcache_nbins;
    JE_ASSERT(nbins <= TCACHE_NBINS_MAX);
    return nbins;
}

/// jemalloc: tcache_max_get
JE_ALWAYS_INLINE size_t tcacheMaxGet(const ThreadCacheSlow * tcache_slow)
{
    JE_ASSERT(tcache_slow != nullptr);
    size_t tcache_max = sz::indexToSize(tcacheNbinsGet(tcache_slow) - 1);
    JE_ASSERT(tcache_max <= TCACHE_MAXCLASS_LIMIT);
    return tcache_max;
}

/// jemalloc: tcache_max_set
JE_ALWAYS_INLINE void tcacheMaxSet(ThreadCacheSlow * tcache_slow, size_t tcache_max)
{
    JE_ASSERT(tcache_slow != nullptr);
    JE_ASSERT(tcache_max <= TCACHE_MAXCLASS_LIMIT);
    tcache_slow->tcache_nbins = sz::sizeToIndex(tcache_max) + 1;
}

/// jemalloc: tcache_bin_settings_backup
JE_ALWAYS_INLINE void tcacheBinSettingsBackup(const ThreadCache * tcache, CacheBinInfo * tcache_bin_info)
{
    for (unsigned i = 0; i < TCACHE_NBINS_MAX; ++i)
        tcache_bin_info[i].init(tcache->bins[i].ncachedMaxGetUnsafe());
}

/// If a bin's ind >= nbins or ncached_max == 0, it must be disabled. If a bin is enabled, it has ind < nbins and
/// ncached_max > 0. In release builds this is just the `stack_head` compare.
/// jemalloc: tcache_bin_disabled
JE_ALWAYS_INLINE bool tcacheBinDisabled(szind_t ind, const CacheBin * bin, [[maybe_unused]] const ThreadCacheSlow * tcache_slow)
{
    JE_ASSERT(bin != nullptr);
    JE_ASSERT(ind < TCACHE_NBINS_MAX);
    bool disabled = bin->disabled();

    if constexpr (config::debug)
    {
        unsigned nbins = tcacheNbinsGet(tcache_slow);
        cache_bin_sz_t ncached_max = bin->ncachedMaxGetUnsafe();
        if (ind >= nbins)
            JE_ASSERT(disabled);
        else
            JE_ASSERT(!disabled || ncached_max == 0);
        if (ncached_max == 0)
            JE_ASSERT(disabled);
        else
            JE_ASSERT(!disabled || ind >= nbins);
        if (disabled)
            JE_ASSERT(ind >= nbins || ncached_max == 0);
        else
            JE_ASSERT(ind < nbins && ncached_max > 0);
    }

    return disabled;
}

/// --- Slow paths (ThreadCache.cpp) ----------------------------------------------------------------------------------

/// jemalloc: tcache_salloc
size_t tcacheSalloc(ThreadState * tsdn, const void * ptr);

/// Fills the (empty) bin from the arena and allocates from it.
/// jemalloc: tcache_alloc_small_hard
void * tcacheAllocSmallHard(ThreadState * tsdn, Arena * arena, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, bool & tcache_success);

/// Flushes the bin down to `rem` cached items (the bottom items are flushed).
/// jemalloc: tcache_bin_flush_small, tcache_bin_flush_large
void tcacheBinFlushSmall(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, unsigned rem);
void tcacheBinFlushLarge(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, unsigned rem);

/// Flushes the stashed (UAF detection) items, after checking their junk. A no-op when nothing is stashed.
/// jemalloc: tcache_bin_flush_stashed
void tcacheBinFlushStashed(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, bool is_small);

/// --- Fast paths (tcache_inlines.h) ---------------------------------------------------------------------------------

/// jemalloc: tcache_alloc_small
JE_ALWAYS_INLINE void * tcacheAllocSmall(
    ThreadState & tsd, Arena * arena, ThreadCache * tcache, size_t size, szind_t binind, bool zero, bool /*slow_path*/)
{
    void * ret;
    bool tcache_success;

    JE_ASSERT(binind < SC_NBINS);
    CacheBin * bin = &tcache->bins[binind];
    ret = bin->alloc(tcache_success);
    JE_ASSERT(tcache_success == (ret != nullptr));
    if (JE_UNLIKELY(!tcache_success))
    {
        bool tcache_hard_success;
        arena = arenaChoose(tsd, arena);
        if (JE_UNLIKELY(arena == nullptr))
            return nullptr;
        if (JE_UNLIKELY(tcacheBinDisabled(binind, bin, tcache->tcache_slow)))
        {
            /// Stats and zero are handled directly by the arena.
            return arenaMallocHard(&tsd, arena, size, binind, zero, /* slab */ true);
        }
        tcacheBinFlushStashed(tsd, tcache, bin, binind, /* is_small */ true);

        ret = tcacheAllocSmallHard(&tsd, arena, tcache, bin, binind, tcache_hard_success);
        if (!tcache_hard_success)
            return nullptr;
    }

    JE_ASSERT(ret);
    if (JE_UNLIKELY(zero))
    {
        size_t usize = sz::indexToSize(binind);
        JE_ASSERT(tcacheSalloc(&tsd, ret) == usize);
        memset(ret, 0, usize);
    }
    if constexpr (config::stats)
        ++bin->tstats.nrequests;
    return ret;
}

/// jemalloc: tcache_alloc_large
JE_ALWAYS_INLINE void * tcacheAllocLarge(
    ThreadState & tsd, Arena * arena, ThreadCache * tcache, size_t size, szind_t binind, bool zero, bool /*slow_path*/)
{
    void * ret;
    bool tcache_success;

    CacheBin * bin = &tcache->bins[binind];
    JE_ASSERT(binind >= SC_NBINS && !tcacheBinDisabled(binind, bin, tcache->tcache_slow));
    ret = bin->alloc(tcache_success);
    JE_ASSERT(tcache_success == (ret != nullptr));
    if (JE_UNLIKELY(!tcache_success))
    {
        /// Only allocate one large object at a time, because it's quite expensive to create one and not use it.
        arena = arenaChoose(tsd, arena);
        if (JE_UNLIKELY(arena == nullptr))
            return nullptr;
        tcacheBinFlushStashed(tsd, tcache, bin, binind, /* is_small */ false);

        ret = largeMalloc(&tsd, arena, sz::s2u(size), zero);
        if (ret == nullptr)
            return nullptr;
    }
    else
    {
        if (JE_UNLIKELY(zero))
        {
            size_t usize = sz::indexToSize(binind);
            JE_ASSERT(usize <= tcacheMaxGet(tcache->tcache_slow));
            memset(ret, 0, usize);
        }

        if constexpr (config::stats)
            ++bin->tstats.nrequests;
    }

    return ret;
}

/// jemalloc: tcache_dalloc_small
JE_ALWAYS_INLINE void tcacheDallocSmall(ThreadState & tsd, ThreadCache * tcache, void * ptr, szind_t binind, bool /*slow_path*/)
{
    JE_ASSERT(tcacheSalloc(&tsd, ptr) <= SC_SMALL_MAXCLASS);

    CacheBin * bin = &tcache->bins[binind];
    /// Not marking the branch unlikely because this is past the free fast path (which handles the most common cases),
    /// i.e. at this point it's often uncommon cases.
    if (cacheBinNonfastAligned(ptr))
    {
        /// Junk unconditionally, even if bin is full.
        sanJunkPtr(ptr, sz::indexToSize(binind));
        if (bin->stash(ptr))
            return;
        JE_ASSERT(bin->full());
        /// Bin full; fall through into the flush branch.
    }

    if (JE_UNLIKELY(!bin->dallocEasy(ptr)))
    {
        if (JE_UNLIKELY(tcacheBinDisabled(binind, bin, tcache->tcache_slow)))
        {
            arenaDallocSmall(&tsd, ptr);
            return;
        }
        cache_bin_sz_t max = bin->ncachedMaxGet();
        unsigned remain = max >> opt.lg_tcache_flush_small_div;
        tcacheBinFlushSmall(tsd, tcache, bin, binind, remain);
        [[maybe_unused]] bool ret = bin->dallocEasy(ptr);
        JE_ASSERT(ret);
    }
}

/// jemalloc: tcache_dalloc_large
JE_ALWAYS_INLINE void tcacheDallocLarge(ThreadState & tsd, ThreadCache * tcache, void * ptr, szind_t binind, bool /*slow_path*/)
{
    JE_ASSERT(tcacheSalloc(&tsd, ptr) > SC_SMALL_MAXCLASS);
    JE_ASSERT(tcacheSalloc(&tsd, ptr) <= tcacheMaxGet(tcache->tcache_slow));
    JE_ASSERT(!tcacheBinDisabled(binind, &tcache->bins[binind], tcache->tcache_slow));

    CacheBin * bin = &tcache->bins[binind];
    if (JE_UNLIKELY(!bin->dallocEasy(ptr)))
    {
        unsigned remain = bin->ncachedMaxGet() >> opt.lg_tcache_flush_large_div;
        tcacheBinFlushLarge(tsd, tcache, bin, binind, remain);
        [[maybe_unused]] bool ret = bin->dallocEasy(ptr);
        JE_ASSERT(ret);
    }
}

/// Creates an explicit tcache (for `tcache.create`, and the re-creation of a flushed one). Returns null on OOM.
/// jemalloc: tcache_create_explicit
ThreadCache * tcacheCreateExplicit(ThreadState & tsd);

/// jemalloc: tcaches_get
JE_ALWAYS_INLINE ThreadCache * tcachesGet(ThreadState & tsd, unsigned ind)
{
    ThreadCaches * elm = &tcaches[ind];
    if (JE_UNLIKELY(elm->tcache == nullptr))
    {
        printMessage("<jemalloc>: invalid tcache id (%u).\n", ind);
        abort();
    }
    else if (JE_UNLIKELY(elm->tcache == TCACHES_ELM_NEED_REINIT))
    {
        elm->tcache = tcacheCreateExplicit(tsd);
    }
    return elm->tcache;
}

/// --- Settings, life cycle (ThreadCache.cpp) ------------------------------------------------------------------------

/// The default `ncached_max` of every bin: computed by `tcacheBoot` (from `opt.tcache_ncached_max` where set,
/// `tcacheNcachedMaxCompute` otherwise); not modified afterwards.
/// jemalloc: tcache_get_default_ncached_max (`opt_tcache_ncached_max` after `tcache_boot`)
const CacheBinInfo * tcacheGetDefaultNcachedMax();

/// Whether `tcache_ncached_max` (malloc_conf) set the bin.
/// jemalloc: tcache_get_default_ncached_max_set
bool tcacheGetDefaultNcachedMaxSet(szind_t ind);

/// The default `ncached_max` of a bin computed from the slab size and the `tcache_nslots_*` options.
/// jemalloc: tcache_ncached_max_compute
unsigned tcacheNcachedMaxCompute(szind_t szind);

/// Computes the values for each bin (bins with indices >= tcache_nbins cache nothing, but get a value too).
/// jemalloc: tcache_bin_info_compute
void tcacheBinInfoCompute(CacheBinInfo * tcache_bin_info);

/// `thread.tcache.ncached_max.read_sizeclass`. Returns true on error (size > `TCACHE_MAXCLASS_LIMIT`).
/// jemalloc: tcache_bin_ncached_max_read
bool tcacheBinNcachedMaxRead(ThreadState & tsd, size_t bin_size, cache_bin_sz_t & ncached_max);

/// `thread.tcache.ncached_max.write`: parses the settings over the current ones and reboots the tcache.
/// Returns true on error. The tcache must be available.
/// jemalloc: tcache_bins_ncached_max_write
bool tcacheBinsNcachedMaxWrite(ThreadState & tsd, const char * settings, size_t len);

/// jemalloc: tcache_arena_associate, tcache_arena_reassociate
void tcacheArenaAssociate(ThreadState * tsdn, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, Arena * arena);
void tcacheArenaReassociate(ThreadState * tsdn, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, Arena * arena);

/// `thread.tcache.max`. Returns true on error.
/// jemalloc: thread_tcache_max_set
bool threadTcacheMaxSet(ThreadState & tsd, size_t tcache_max);

/// Destroys the automatic tcache of the thread (if available) and resets its bins to the zero state.
/// jemalloc: tcache_cleanup
void tcacheCleanup(ThreadState & tsd);

/// Merges and resets the tcache request counters into the arena stats.
/// jemalloc: tcache_stats_merge
void tcacheStatsMerge(ThreadState * tsdn, ThreadCache * tcache, Arena * arena);

/// jemalloc: tcaches_create (returns true on error), tcaches_flush, tcaches_destroy
bool tcachesCreate(ThreadState & tsd, Base * base, unsigned & r_ind);
void tcachesFlush(ThreadState & tsd, unsigned ind);
void tcachesDestroy(ThreadState & tsd, unsigned ind);

/// Returns true on error.
/// jemalloc: tcache_boot
bool tcacheBoot(ThreadState * tsdn, Base * base);

/// jemalloc: tcache_prefork, tcache_postfork_parent, tcache_postfork_child
void tcachePrefork(ThreadState * tsdn);
void tcachePostforkParent(ThreadState * tsdn);
void tcachePostforkChild(ThreadState * tsdn);

/// Flushes every enabled bin of the automatic tcache (`thread.tcache.flush`, `thread.idle`). The tcache must be
/// available.
/// jemalloc: tcache_flush
void tcacheFlush(ThreadState & tsd);

/// `tcacheTsdDataInit` (ThreadState.h) is jemalloc's `tsd_tcache_enabled_data_init`.

/// `thread.tcache.enabled`.
/// jemalloc: tcache_enabled_set
void tcacheEnabledSet(ThreadState & tsd, bool enabled);

/// The allocation of the cache bin stacks of the automatic tcache. A function pointer (as in jemalloc) so that tests
/// can inject failures.
/// jemalloc: tcache_stack_alloc
extern constinit void * (*tcache_stack_alloc)(ThreadState * tsdn, size_t size, size_t alignment);

/// --- Internals exposed for the tests ------------------------------------------------------------------------------

namespace tcache_detail
{

/// jemalloc: tcache_bin_fill_ctl_init, tcache_bin_fill_ctl_get
void tcacheBinFillCtlInit(ThreadCacheSlow * tcache_slow, szind_t szind);
CacheBinFillCtl * tcacheBinFillCtlGet(ThreadCacheSlow * tcache_slow, szind_t szind);
/// jemalloc: tcache_nfill_small_lg_div_get
uint8_t tcacheNfillSmallLgDivGet(ThreadCacheSlow * tcache_slow, szind_t szind);
/// jemalloc: tcache_nfill_small_burst_prepare, tcache_nfill_small_burst_reset
void tcacheNfillSmallBurstPrepare(ThreadCacheSlow * tcache_slow, szind_t szind);
void tcacheNfillSmallBurstReset(ThreadCacheSlow * tcache_slow, szind_t szind);
/// jemalloc: tcache_nfill_small_gc_update
void tcacheNfillSmallGcUpdate(ThreadCacheSlow * tcache_slow, szind_t szind, cache_bin_sz_t limit);
/// jemalloc: tcache_gc_item_delay_compute
uint8_t tcacheGcItemDelayCompute(szind_t szind);
/// jemalloc: tcache_gc_is_addr_remote
bool tcacheGcIsAddrRemote(void * addr, uintptr_t min, uintptr_t max);
/// jemalloc: tcache_gc_small_nremote_get
cache_bin_sz_t tcacheGcSmallNremoteGet(
    CacheBin * cache_bin, void * addr, uintptr_t & addr_min, uintptr_t & addr_max, szind_t szind, size_t nflush);
/// jemalloc: tcache_gc_small_bin_shuffle
void tcacheGcSmallBinShuffle(CacheBin * cache_bin, cache_bin_sz_t nremote, uintptr_t addr_min, uintptr_t addr_max);

}

/// The GC event handler entry points (ThreadEvent.h): `tcacheGcNewEventWait`, `tcacheGcPostponedEventWait`,
/// `tcacheGcEvent`.

}
