#include <allocator/ThreadCache.h>

#include <allocator/ArenaInlines.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Conf.h>
#include <allocator/Frontend.h>
#include <allocator/Mutex.h>
#include <allocator/ThreadEvent.h>

namespace jemalloc
{

using namespace tcache_detail;

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit unsigned global_do_not_change_tcache_nbins = 0;
constinit size_t global_do_not_change_tcache_maxclass = 0;
constinit ThreadCaches * tcaches = nullptr;

namespace
{

/// Default bin info for each bin: initialized from `opt.tcache_ncached_max` (malloc_conf) and the computed defaults
/// in `tcacheBoot`; not modified after that.
/// jemalloc: opt_tcache_ncached_max (after `tcache_boot`)
constinit CacheBinInfo tcache_default_ncached_max[TCACHE_NBINS_MAX] = {};

/// Index of the first element within `tcaches` that has never been used.
/// jemalloc: tcaches_past
constinit unsigned tcaches_past = 0;

/// Head of the singly linked list tracking available `tcaches` elements.
/// jemalloc: tcaches_avail
constinit ThreadCaches * tcaches_avail = nullptr;

/// Protects `tcaches`, `tcaches_past`, `tcaches_avail`.
/// jemalloc: tcaches_mtx (WITNESS_RANK_TCACHES)
constinit Mutex tcaches_mtx;

}

/// jemalloc: tcache_salloc
size_t tcacheSalloc(ThreadState * tsdn, const void * ptr)
{
    return arenaSalloc(tsdn, ptr);
}

/// --- GC event wait functions ---------------------------------------------------------------------------------------

/// jemalloc: tcache_gc_new_event_wait
uint64_t tcacheGcNewEventWait(ThreadState & /*tsd*/)
{
    return opt.tcache_gc_incr_bytes;
}

/// jemalloc: tcache_gc_postponed_event_wait
uint64_t tcacheGcPostponedEventWait(ThreadState & /*tsd*/)
{
    return TE_MIN_START_WAIT;
}

/// `tcache_gc_dalloc_new_event_wait` / `tcache_gc_dalloc_postponed_event_wait` are dead in jemalloc (the same handler
/// is registered for alloc and dalloc) and are not ported.

/// --- Fill count control ---------------------------------------------------------------------------------------------

namespace tcache_detail
{

/// jemalloc: tcache_bin_fill_ctl_init
void tcacheBinFillCtlInit(ThreadCacheSlow * tcache_slow, szind_t szind)
{
    JE_ASSERT(szind < SC_NBINS);
    CacheBinFillCtl * ctl = &tcache_slow->bin_fill_ctl_do_not_access_directly[szind];
    ctl->base = 1;
    ctl->offset = 0;
}

/// jemalloc: tcache_bin_fill_ctl_get
CacheBinFillCtl * tcacheBinFillCtlGet(ThreadCacheSlow * tcache_slow, szind_t szind)
{
    JE_ASSERT(szind < SC_NBINS);
    CacheBinFillCtl * ctl = &tcache_slow->bin_fill_ctl_do_not_access_directly[szind];
    JE_ASSERT(ctl->base > ctl->offset);
    return ctl;
}

/// The number of items to be filled at a time for a given small bin is `ncached_max >> lg_fill_div`, where
/// `lg_fill_div = base - offset`. The base is adjusted during GC based on the traffic within a period of time, while
/// the offset is updated in real time to handle the immediate traffic.
/// jemalloc: tcache_nfill_small_lg_div_get
uint8_t tcacheNfillSmallLgDivGet(ThreadCacheSlow * tcache_slow, szind_t szind)
{
    CacheBinFillCtl * ctl = tcacheBinFillCtlGet(tcache_slow, szind);
    return static_cast<uint8_t>(ctl->base - (opt.experimental_tcache_gc ? ctl->offset : 0));
}

/// When we want to fill more items to respond to burst load, the offset is increased so that (base - offset)
/// decreases, which in turn increases the number of items to be filled.
/// jemalloc: tcache_nfill_small_burst_prepare
void tcacheNfillSmallBurstPrepare(ThreadCacheSlow * tcache_slow, szind_t szind)
{
    CacheBinFillCtl * ctl = tcacheBinFillCtlGet(tcache_slow, szind);
    if (ctl->offset + 1 < ctl->base)
        ++ctl->offset;
}

/// jemalloc: tcache_nfill_small_burst_reset
void tcacheNfillSmallBurstReset(ThreadCacheSlow * tcache_slow, szind_t szind)
{
    CacheBinFillCtl * ctl = tcacheBinFillCtlGet(tcache_slow, szind);
    ctl->offset = 0;
}

/// limit == 0: the fill count should be increased, i.e. lg_div (base) should be decreased.
/// limit != 0: limit is ncached_max, the fill count should be decreased, i.e. lg_div (base) should be increased.
/// jemalloc: tcache_nfill_small_gc_update
void tcacheNfillSmallGcUpdate(ThreadCacheSlow * tcache_slow, szind_t szind, cache_bin_sz_t limit)
{
    CacheBinFillCtl * ctl = tcacheBinFillCtlGet(tcache_slow, szind);
    if (!limit && ctl->base > 1)
    {
        /// Increase fill count by 2X for small bins. Make sure lg_fill_div stays greater than 1.
        --ctl->base;
    }
    else if (limit && (limit >> ctl->base) > 1)
    {
        /// Reduce fill count by 2X. Limit lg_fill_div such that the fill count is always at least 1.
        ++ctl->base;
    }
    /// Reset the offset for the next GC period.
    ctl->offset = 0;
}

/// jemalloc: tcache_gc_item_delay_compute
uint8_t tcacheGcItemDelayCompute(szind_t szind)
{
    JE_ASSERT(szind < SC_NBINS);
    size_t sz = sz::indexToSize(szind);
    size_t item_delay = opt.tcache_gc_delay_bytes / sz;
    size_t delay_max = size_t(1) << (sizeof(ThreadCacheSlow::bin_flush_delay_items[0]) * 8);
    if (item_delay >= delay_max)
        item_delay = delay_max - 1;
    return static_cast<uint8_t>(item_delay);
}

/// --- GC ------------------------------------------------------------------------------------------------------------

/// jemalloc: tcache_gc_is_addr_remote
bool tcacheGcIsAddrRemote(void * addr, uintptr_t min, uintptr_t max)
{
    JE_ASSERT(addr != nullptr);
    return reinterpret_cast<uintptr_t>(addr) < min || reinterpret_cast<uintptr_t>(addr) >= max;
}

/// Counts the cached pointers that are remote w.r.t. the slab at `addr` or its 2 MiB neighborhood, and selects the
/// range to keep.
/// jemalloc: tcache_gc_small_nremote_get
cache_bin_sz_t tcacheGcSmallNremoteGet(
    CacheBin * cache_bin, void * addr, uintptr_t & addr_min, uintptr_t & addr_max, szind_t szind, size_t nflush)
{
    JE_ASSERT(addr != nullptr);
    /// The slab address range that the provided addr belongs to.
    uintptr_t slab_min = reinterpret_cast<uintptr_t>(addr);
    uintptr_t slab_max = slab_min + bin_infos[szind].slab_size;
    /// When growing retained virtual memory, it's increased exponentially, starting from 2M, so that the total number
    /// of disjoint virtual memory ranges retained by each shard is limited.
    uintptr_t neighbor_min = (reinterpret_cast<uintptr_t>(addr) > TCACHE_GC_NEIGHBOR_LIMIT)
        ? (reinterpret_cast<uintptr_t>(addr) - TCACHE_GC_NEIGHBOR_LIMIT)
        : 0;
    uintptr_t neighbor_max = (reinterpret_cast<uintptr_t>(addr) < (UINTPTR_MAX - TCACHE_GC_NEIGHBOR_LIMIT))
        ? (reinterpret_cast<uintptr_t>(addr) + TCACHE_GC_NEIGHBOR_LIMIT)
        : UINTPTR_MAX;

    /// Scan the entire bin to count the number of remote pointers.
    void ** head = cache_bin->stack_head;
    cache_bin_sz_t n_remote_slab = 0;
    cache_bin_sz_t n_remote_neighbor = 0;
    cache_bin_sz_t ncached = cache_bin->ncachedGetLocal();
    for (void ** cur = head; cur < head + ncached; ++cur)
    {
        n_remote_slab = static_cast<cache_bin_sz_t>(n_remote_slab + tcacheGcIsAddrRemote(*cur, slab_min, slab_max));
        n_remote_neighbor = static_cast<cache_bin_sz_t>(n_remote_neighbor + tcacheGcIsAddrRemote(*cur, neighbor_min, neighbor_max));
    }
    /// Since the slab size is dynamic and can be larger than 2M (`TCACHE_GC_NEIGHBOR_LIMIT`), there is no guarantee
    /// as to which of `n_remote_slab` and `n_remote_neighbor` is greater.
    JE_ASSERT(n_remote_slab <= ncached && n_remote_neighbor <= ncached);
    /// We first consider keeping ptrs from the neighboring addr range, since in most cases the range is greater than
    /// the slab range. So if the number of non-neighbor ptrs is more than the intended flush amount, we use it as the
    /// anchor for flushing.
    if (n_remote_neighbor >= nflush)
    {
        addr_min = neighbor_min;
        addr_max = neighbor_max;
        return n_remote_neighbor;
    }
    /// We then consider only keeping ptrs from the local slab, and in most cases this is stricter, assuming that
    /// slab < 2M is the common case.
    addr_min = slab_min;
    addr_max = slab_max;
    return n_remote_slab;
}

/// Shuffles the ptrs in the bin to put the remote pointers at the bottom; the local ones move to the top keeping
/// their relative order.
/// jemalloc: tcache_gc_small_bin_shuffle
void tcacheGcSmallBinShuffle(CacheBin * cache_bin, cache_bin_sz_t nremote, uintptr_t addr_min, uintptr_t addr_max)
{
    void ** swap = nullptr;
    cache_bin_sz_t ncached = cache_bin->ncachedGetLocal();
    cache_bin_sz_t ntop = static_cast<cache_bin_sz_t>(ncached - nremote);
    cache_bin_sz_t cnt = 0;
    JE_ASSERT(ntop > 0 && ntop < ncached);
    /// Scan the [head, head + ntop) part of the cache bin, bubbling the non-remote ptrs to the top of the bin. After
    /// this, [head, head + cnt) contains only non-remote ptrs, in the same relative order as before, while
    /// [head + cnt, head + ntop) contains only remote ptrs.
    void ** head = cache_bin->stack_head;
    for (void ** cur = head; cur < head + ntop; ++cur)
    {
        if (!tcacheGcIsAddrRemote(*cur, addr_min, addr_max))
        {
            /// Tracks the number of non-remote ptrs seen so far.
            ++cnt;
            /// There is a remote ptr before the current non-remote ptr: swap them, and increment the swap pointer so
            /// that it still points to the top remote ptr in the bin.
            if (swap != nullptr)
            {
                JE_ASSERT(swap < cur);
                JE_ASSERT(tcacheGcIsAddrRemote(*swap, addr_min, addr_max));
                void * tmp = *cur;
                *cur = *swap;
                *swap = tmp;
                ++swap;
                JE_ASSERT(swap <= cur);
                JE_ASSERT(tcacheGcIsAddrRemote(*swap, addr_min, addr_max));
            }
            continue;
        }
        else if (swap == nullptr)
        {
            /// Swap always points to the top remote ptr in the bin.
            swap = cur;
        }
    }
    /// Scan the [head + ntop, head + ncached) part of the cache bin, after which it should only contain remote ptrs.
    for (void ** cur = head + ntop; cur < head + ncached; ++cur)
    {
        /// Early break if all non-remote ptrs have been moved.
        if (cnt == ntop)
            break;
        if (!tcacheGcIsAddrRemote(*cur, addr_min, addr_max))
        {
            JE_ASSERT(tcacheGcIsAddrRemote(*(head + cnt), addr_min, addr_max));
            void * tmp = *cur;
            *cur = *(head + cnt);
            *(head + cnt) = tmp;
            ++cnt;
        }
    }
    JE_ASSERT(cnt == ntop);
    /// Sanity check to make sure the shuffle is done correctly.
    if constexpr (config::debug)
    {
        for (void ** cur = head; cur < head + ncached; ++cur)
        {
            JE_ASSERT(*cur != nullptr);
            JE_ASSERT(
                ((cur < head + ntop) && !tcacheGcIsAddrRemote(*cur, addr_min, addr_max))
                || ((cur >= head + ntop) && tcacheGcIsAddrRemote(*cur, addr_min, addr_max)));
        }
    }
}

}

namespace
{

/// The base address of the arena's current slab of the bin (`slabcur`, else the first nonfull slab), or null.
/// jemalloc: tcache_gc_small_heuristic_addr_get
inline void * tcacheGcSmallHeuristicAddrGet(ThreadState & tsd, ThreadCacheSlow * tcache_slow, szind_t szind)
{
    JE_ASSERT(szind < SC_NBINS);
    ThreadState * tsdn = &tsd;
    Bin * bin = binChoose(tsdn, tcache_slow->arena, szind, nullptr);
    JE_ASSERT(bin != nullptr);

    bin->lock.lock(tsdn);
    Extent * slab = (bin->slabcur == nullptr) ? bin->slabs_nonfull.first() : bin->slabcur;
    JE_ASSERT(slab != nullptr || bin->slabs_nonfull.empty());
    void * ret = (slab != nullptr) ? slab->addr() : nullptr;
    JE_ASSERT(ret != nullptr || slab == nullptr);
    bin->lock.unlock(tsdn);

    return ret;
}

/// Aims to flush 3/4 of the items below low-water, with remote pointers being prioritized for flushing.
/// jemalloc: tcache_gc_small
bool tcacheGcSmall(ThreadState & tsd, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, szind_t szind)
{
    JE_ASSERT(szind < SC_NBINS);

    CacheBin * cache_bin = &tcache->bins[szind];
    JE_ASSERT(!tcacheBinDisabled(szind, cache_bin, tcache->tcache_slow));
    cache_bin_sz_t ncached = cache_bin->ncachedGetLocal();
    cache_bin_sz_t low_water = cache_bin->lowWaterGet();
    if (low_water > 0)
    {
        /// There are unused items within the GC period => reduce the fill count. The limit != 0 is borrowed to
        /// indicate that the fill count should be reduced.
        tcacheNfillSmallGcUpdate(tcache_slow, szind, /* limit */ cache_bin->ncachedMaxGet());
    }
    else if (tcache_slow->bin_refilled[szind])
    {
        /// There have been refills within the GC period => increase the fill count. The limit set to 0 is borrowed
        /// to indicate that the fill count should be increased.
        tcacheNfillSmallGcUpdate(tcache_slow, szind, /* limit */ 0);
        tcache_slow->bin_refilled[szind] = false;
    }
    JE_ASSERT(!tcache_slow->bin_refilled[szind]);

    cache_bin_sz_t nflush = static_cast<cache_bin_sz_t>(low_water - (low_water >> 2));
    /// When the new tcache gc is not enabled, keep the flush delay logic, and directly flush the bottom nflush items
    /// if needed.
    if (!opt.experimental_tcache_gc)
    {
        if (nflush < tcache_slow->bin_flush_delay_items[szind])
        {
            uint8_t nflush_uint8 = static_cast<uint8_t>(nflush);
            tcache_slow->bin_flush_delay_items[szind] = static_cast<uint8_t>(tcache_slow->bin_flush_delay_items[szind] - nflush_uint8);
            return false;
        }

        tcache_slow->bin_flush_delay_items[szind] = tcacheGcItemDelayCompute(szind);
        goto label_flush;
    }

    {
        /// Directly go to the flush path when the entire bin needs to be flushed.
        if (nflush == ncached)
            goto label_flush;

        /// Query the arena binshard to get heuristic locality info.
        void * addr = tcacheGcSmallHeuristicAddrGet(tsd, tcache_slow, szind);
        if (addr == nullptr)
            goto label_flush;

        /// Use the queried addr above to get the number of remote ptrs in the bin, and the min/max of the local addr
        /// range.
        uintptr_t addr_min;
        uintptr_t addr_max;
        cache_bin_sz_t nremote = tcacheGcSmallNremoteGet(cache_bin, addr, addr_min, addr_max, szind, nflush);

        /// Update nflush to the larger of the intended flush count and the number of remote ptrs.
        if (nremote > nflush)
            nflush = nremote;
        /// When entering the locality check, nflush should be less than ncached, otherwise the entire bin should be
        /// flushed regardless. The only case when nflush gets updated to ncached after the locality check is when
        /// all the items in the bin are remote, in which case the entire bin should also be flushed.
        JE_ASSERT(nflush < ncached || nremote == ncached);
        if (nremote == 0 || nremote == ncached)
            goto label_flush;

        /// Move the remote pointers to the bottom of the bin for flushing. As long as moved to the bottom, the order
        /// of these nremote ptrs does not matter, since they are going to be flushed anyway. The rest of the ptrs are
        /// moved to the top of the bin, and their relative order is maintained.
        tcacheGcSmallBinShuffle(cache_bin, nremote, addr_min, addr_max);
    }

label_flush:
    if (nflush == 0)
    {
        JE_ASSERT(low_water == 0);
        return false;
    }
    JE_ASSERT(nflush <= ncached);
    tcacheBinFlushSmall(tsd, tcache, cache_bin, szind, static_cast<unsigned>(ncached - nflush));
    return true;
}

/// Like the small GC, flushes 3/4 of the untouched items; but simply the bottom ones, without any locality check.
/// jemalloc: tcache_gc_large
bool tcacheGcLarge(ThreadState & tsd, ThreadCacheSlow * /*tcache_slow*/, ThreadCache * tcache, szind_t szind)
{
    JE_ASSERT(szind >= SC_NBINS);
    CacheBin * cache_bin = &tcache->bins[szind];
    JE_ASSERT(!tcacheBinDisabled(szind, cache_bin, tcache->tcache_slow));
    cache_bin_sz_t low_water = cache_bin->lowWaterGet();
    if (low_water == 0)
        return false;
    unsigned nrem = static_cast<unsigned>(cache_bin->ncachedGetLocal() - low_water + (low_water >> 2));
    tcacheBinFlushLarge(tsd, tcache, cache_bin, szind, nrem);
    return true;
}

/// Tries to GC one bin; returns true if some items were flushed.
/// jemalloc: tcache_try_gc_bin
bool tcacheTryGcBin(ThreadState & tsd, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, szind_t szind)
{
    JE_ASSERT(tcache != nullptr);
    CacheBin * cache_bin = &tcache->bins[szind];
    if (tcacheBinDisabled(szind, cache_bin, tcache_slow))
        return false;

    bool is_small = (szind < SC_NBINS);
    tcacheBinFlushStashed(tsd, tcache, cache_bin, szind, is_small);
    bool ret = is_small ? tcacheGcSmall(tsd, tcache_slow, tcache, szind) : tcacheGcLarge(tsd, tcache_slow, tcache, szind);
    cache_bin->lowWaterSet();
    return ret;
}

}

/// jemalloc: tcache_gc_event
void tcacheGcEvent(ThreadState & tsd)
{
    ThreadCache * tcache = tcacheGet(tsd);
    if (tcache == nullptr)
        return;

    ThreadCacheSlow * tcache_slow = tsd.tcacheSlowGet();
    JE_ASSERT(tcache_slow != nullptr);

    /// When the new tcache gc is not enabled, GC one bin at a time.
    if (!opt.experimental_tcache_gc)
    {
        szind_t szind = tcache_slow->next_gc_bin;
        tcacheTryGcBin(tsd, tcache_slow, tcache, szind);
        ++tcache_slow->next_gc_bin;
        if (tcache_slow->next_gc_bin == tcacheNbinsGet(tcache_slow))
            tcache_slow->next_gc_bin = 0;
        return;
    }

    NsTime now = tcache_slow->last_gc_time;
    now.update();
    JE_ASSERT(now.compare(tcache_slow->last_gc_time) >= 0);

    if (now.ns() - tcache_slow->last_gc_time.ns() < TCACHE_GC_INTERVAL_NS)
    {
        /// The time interval is too short, skip this event.
        return;
    }
    /// Update last_gc_time to now.
    tcache_slow->last_gc_time = now;

    unsigned gc_small_nbins = 0;
    unsigned gc_large_nbins = 0;
    unsigned tcache_nbins = tcacheNbinsGet(tcache_slow);
    unsigned small_nbins = tcache_nbins > SC_NBINS ? SC_NBINS : tcache_nbins;
    szind_t szind_small = tcache_slow->next_gc_bin_small;
    szind_t szind_large = tcache_slow->next_gc_bin_large;

    /// Flush at most `TCACHE_GC_SMALL_NBINS_MAX` small bins at a time.
    for (unsigned i = 0; i < small_nbins && gc_small_nbins < TCACHE_GC_SMALL_NBINS_MAX; ++i)
    {
        JE_ASSERT(szind_small < SC_NBINS);
        if (tcacheTryGcBin(tsd, tcache_slow, tcache, szind_small))
            ++gc_small_nbins;
        if (++szind_small == small_nbins)
            szind_small = 0;
    }
    tcache_slow->next_gc_bin_small = szind_small;

    if (tcache_nbins <= SC_NBINS)
        return;

    /// Flush at most `TCACHE_GC_LARGE_NBINS_MAX` large bins at a time.
    for (unsigned i = SC_NBINS; i < tcache_nbins && gc_large_nbins < TCACHE_GC_LARGE_NBINS_MAX; ++i)
    {
        JE_ASSERT(szind_large >= SC_NBINS && szind_large < tcache_nbins);
        if (tcacheTryGcBin(tsd, tcache_slow, tcache, szind_large))
            ++gc_large_nbins;
        if (++szind_large == tcache_nbins)
            szind_large = SC_NBINS;
    }
    tcache_slow->next_gc_bin_large = szind_large;
}

/// --- Fill and flush ------------------------------------------------------------------------------------------------

/// jemalloc: tcache_alloc_small_hard
void * tcacheAllocSmallHard(ThreadState * tsdn, Arena * arena, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, bool & tcache_success)
{
    ThreadCacheSlow * tcache_slow = tcache->tcache_slow;
    void * ret;

    JE_ASSERT(tcache_slow->arena != nullptr);
    JE_ASSERT(!tcacheBinDisabled(binind, cache_bin, tcache_slow));
    JE_ASSERT(cache_bin->ncachedGetLocal() == 0);
    cache_bin_sz_t nfill = static_cast<cache_bin_sz_t>(cache_bin->ncachedMaxGet() >> tcacheNfillSmallLgDivGet(tcache_slow, binind));
    if (nfill == 0)
        nfill = 1;
    cache_bin_sz_t nfill_min = opt.experimental_tcache_gc ? static_cast<cache_bin_sz_t>((nfill >> 1) + 1) : nfill;
    cache_bin_sz_t nfill_max = nfill;
    CacheBinPtrArray ptrs(nfill_max);
    cache_bin->initPtrArrayForFill(ptrs, nfill_max);

    cache_bin_sz_t filled = arenaPtrArrayFillSmall(
        tsdn, arena, binind, &ptrs, /* nfill_min */ nfill_min, /* nfill_max */ nfill_max, cache_bin->tstats);
    cache_bin->finishFill(ptrs, filled);
    JE_ASSERT(filled >= nfill_min && filled <= nfill_max);
    JE_ASSERT(cache_bin->ncachedGetLocal() == filled);

    tcache_slow->bin_refilled[binind] = true;
    tcacheNfillSmallBurstPrepare(tcache_slow, binind);
    ret = cache_bin->alloc(tcache_success);

    return ret;
}

namespace
{

/// jemalloc: tcache_bin_flush_bottom
JE_ALWAYS_INLINE void tcacheBinFlushBottom(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, unsigned rem, bool small)
{
    JE_ASSERT(rem <= cache_bin->ncachedMaxGet());
    JE_ASSERT(!tcacheBinDisabled(binind, cache_bin, tcache->tcache_slow));
    [[maybe_unused]] cache_bin_sz_t orig_nstashed = cache_bin->nstashedGetLocal();
    tcacheBinFlushStashed(tsd, tcache, cache_bin, binind, small);

    cache_bin_sz_t ncached = cache_bin->ncachedGetLocal();
    JE_ASSERT(static_cast<cache_bin_sz_t>(rem) <= ncached + orig_nstashed);
    if (static_cast<cache_bin_sz_t>(rem) > ncached)
    {
        /// The stashed flush above could have done enough flushing, if there were many items stashed. Validate
        /// that: 1) non zero stashed, and 2) the bin stack has available space now.
        JE_ASSERT(orig_nstashed > 0);
        JE_ASSERT(ncached + cache_bin->nstashedGetLocal() < cache_bin->ncachedMaxGet());
        /// Still go through the flush logic for stats purpose only.
        rem = ncached;
    }
    cache_bin_sz_t nflush = static_cast<cache_bin_sz_t>(ncached - static_cast<cache_bin_sz_t>(rem));

    CacheBinPtrArray ptrs(nflush);
    cache_bin->initPtrArrayForFlush(ptrs, nflush);

    arenaPtrArrayFlush(tsd, binind, &ptrs, nflush, small, tcache->tcache_slow->arena, cache_bin->tstats);

    cache_bin->finishFlush(ptrs, nflush);
}

}

/// jemalloc: tcache_bin_flush_small
void tcacheBinFlushSmall(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, unsigned rem)
{
    tcacheNfillSmallBurstReset(tcache->tcache_slow, binind);
    tcacheBinFlushBottom(tsd, tcache, cache_bin, binind, rem, /* small */ true);
}

/// jemalloc: tcache_bin_flush_large
void tcacheBinFlushLarge(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, unsigned rem)
{
    tcacheBinFlushBottom(tsd, tcache, cache_bin, binind, rem, /* small */ false);
}

/// Flushing stashed happens on 1) tcache fill, 2) tcache flush, or 3) tcache GC event. This makes sure that the
/// stashed items do not hold memory for too long, and new buffers can only be allocated when nothing is stashed.
///
/// The downside is, the time between stash and flush may be relatively short, especially when the request rate is
/// high. It lowers the chance of detecting write-after-free -- however that is a delayed detection anyway, and is less
/// of a focus than the memory overhead.
/// jemalloc: tcache_bin_flush_stashed
void tcacheBinFlushStashed(ThreadState & tsd, ThreadCache * tcache, CacheBin * cache_bin, szind_t binind, bool is_small)
{
    JE_ASSERT(!tcacheBinDisabled(binind, cache_bin, tcache->tcache_slow));
    /// The two below are for assertion only. The content of the original cached items remains unchanged -- the
    /// stashed items reside on the other end of the stack. Checking the stack head and ncached to verify.
    [[maybe_unused]] void * head_content = *cache_bin->stack_head;
    [[maybe_unused]] cache_bin_sz_t orig_cached = cache_bin->ncachedGetLocal();

    cache_bin_sz_t nstashed = cache_bin->nstashedGetLocal();
    JE_ASSERT(orig_cached + nstashed <= cache_bin->ncachedMaxGet());
    if (nstashed == 0)
        return;

    CacheBinPtrArray ptrs(nstashed);
    cache_bin->initPtrArrayForStashed(binind, ptrs, nstashed);
    sanCheckStashedPtrs(ptrs.ptr, nstashed, sz::indexToSize(binind));
    arenaPtrArrayFlush(tsd, binind, &ptrs, nstashed, is_small, tcache->tcache_slow->arena, cache_bin->tstats);
    cache_bin->finishFlushStashed();

    JE_ASSERT(cache_bin->nstashedGetLocal() == 0);
    JE_ASSERT(cache_bin->ncachedGetLocal() == orig_cached);
    JE_ASSERT(head_content == *cache_bin->stack_head);
}

/// --- Bin settings --------------------------------------------------------------------------------------------------

/// jemalloc: tcache_get_default_ncached_max_set
bool tcacheGetDefaultNcachedMaxSet(szind_t ind)
{
    return opt.tcache_ncached_max_set[ind];
}

/// jemalloc: tcache_get_default_ncached_max
const CacheBinInfo * tcacheGetDefaultNcachedMax()
{
    return tcache_default_ncached_max;
}

/// jemalloc: tcache_bin_ncached_max_read
bool tcacheBinNcachedMaxRead(ThreadState & tsd, size_t bin_size, cache_bin_sz_t & ncached_max)
{
    if (bin_size > TCACHE_MAXCLASS_LIMIT)
        return true;

    if (!tcacheAvailable(tsd))
    {
        ncached_max = 0;
        return false;
    }

    ThreadCache * tcache = tsd.tcacheGet();
    JE_ASSERT(tcache != nullptr);
    szind_t bin_ind = sz::sizeToIndex(bin_size);

    CacheBin * bin = &tcache->bins[bin_ind];
    ncached_max = tcacheBinDisabled(bin_ind, bin, tcache->tcache_slow) ? 0 : bin->ncachedMaxGet();
    return false;
}

/// --- Arena association ---------------------------------------------------------------------------------------------

/// jemalloc: tcache_arena_associate
void tcacheArenaAssociate(ThreadState * tsdn, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, Arena * arena)
{
    JE_ASSERT(tcache_slow->arena == nullptr);
    tcache_slow->arena = arena;

    if constexpr (config::stats)
    {
        /// Link into the list of extant tcaches.
        arena->tcache_ql_mtx.lock(tsdn);

        arena->tcache_ql.elementInit(tcache_slow);
        arena->tcache_ql.tailInsert(tcache_slow);
        tcache_slow->cache_bin_array_descriptor.init(tcache->bins);
        arena->cache_bin_array_descriptor_ql.tailInsert(&tcache_slow->cache_bin_array_descriptor);

        arena->tcache_ql_mtx.unlock(tsdn);
    }
}

namespace
{

/// jemalloc: tcache_arena_dissociate
void tcacheArenaDissociate(ThreadState * tsdn, ThreadCacheSlow * tcache_slow, ThreadCache * /*tcache*/)
{
    Arena * arena = tcache_slow->arena;
    JE_ASSERT(arena != nullptr);
    if constexpr (config::stats)
    {
        /// Unlink from the list of extant tcaches.
        arena->tcache_ql_mtx.lock(tsdn);
        if constexpr (config::debug)
        {
            bool in_ql = false;
            arena->tcache_ql.forEach(
                [&](ThreadCacheSlow * iter)
                {
                    if (iter == tcache_slow)
                        in_ql = true;
                });
            JE_ASSERT(in_ql);
        }
        arena->tcache_ql.remove(tcache_slow);
        arena->cache_bin_array_descriptor_ql.remove(&tcache_slow->cache_bin_array_descriptor);
        tcacheStatsMerge(tsdn, tcache_slow->tcache, arena);
        arena->tcache_ql_mtx.unlock(tsdn);
    }
    tcache_slow->arena = nullptr;
}

}

/// jemalloc: tcache_arena_reassociate
void tcacheArenaReassociate(ThreadState * tsdn, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, Arena * arena)
{
    tcacheArenaDissociate(tsdn, tcache_slow, tcache);
    tcacheArenaAssociate(tsdn, tcache_slow, tcache, arena);
}

/// --- Initialization ------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tcache_default_settings_init
void tcacheDefaultSettingsInit(ThreadCacheSlow * tcache_slow)
{
    JE_ASSERT(tcache_slow != nullptr);
    JE_ASSERT(global_do_not_change_tcache_maxclass != 0);
    JE_ASSERT(global_do_not_change_tcache_nbins != 0);
    tcache_slow->tcache_nbins = global_do_not_change_tcache_nbins;
}

/// jemalloc: tcache_init
void tcacheInit(ThreadState & /*tsd*/, ThreadCacheSlow * tcache_slow, ThreadCache * tcache, void * mem, const CacheBinInfo * tcache_bin_info)
{
    tcache->tcache_slow = tcache_slow;
    tcache_slow->tcache = tcache;

    tcache_slow->link = {};
    tcache_slow->last_gc_time = NsTime::zero();
    tcache_slow->next_gc_bin = 0;
    tcache_slow->next_gc_bin_small = 0;
    tcache_slow->next_gc_bin_large = SC_NBINS;
    tcache_slow->arena = nullptr;
    tcache_slow->dyn_alloc = mem;

    /// We reserve cache bins for all small size classes, even if some may not get used (i.e. bins higher than
    /// tcache_nbins). This allows the fast and common paths to access cache bin metadata safely w/o worrying about
    /// which ones are disabled.
    unsigned tcache_nbins = tcacheNbinsGet(tcache_slow);
    size_t cur_offset = 0;
    cacheBinPreincrement(tcache_bin_info, tcache_nbins, mem, cur_offset);
    for (unsigned i = 0; i < tcache_nbins; ++i)
    {
        if (i < SC_NBINS)
        {
            tcacheBinFillCtlInit(tcache_slow, i);
            tcache_slow->bin_refilled[i] = false;
            tcache_slow->bin_flush_delay_items[i] = tcacheGcItemDelayCompute(i);
        }
        CacheBin * cache_bin = &tcache->bins[i];
        if (tcache_bin_info[i].ncached_max > 0)
            cache_bin->init(tcache_bin_info[i], mem, cur_offset);
        else
            cache_bin->initDisabled(tcache_bin_info[i].ncached_max);
    }
    /// Initialize all disabled bins to a state that can safely and efficiently fail all fastpath alloc / free, so
    /// that no additional check around tcache_nbins is needed on the fast path. Yet we still store the ncached_max in
    /// the bin_info for future usage.
    for (unsigned i = tcache_nbins; i < TCACHE_NBINS_MAX; ++i)
    {
        CacheBin * cache_bin = &tcache->bins[i];
        cache_bin->initDisabled(tcache_bin_info[i].ncached_max);
        JE_ASSERT(tcacheBinDisabled(i, cache_bin, tcache->tcache_slow));
    }

    cacheBinPostincrement(mem, cur_offset);
    if constexpr (config::debug)
    {
        /// Sanity check that the whole stack is used.
        size_t size;
        size_t alignment;
        cacheBinInfoComputeAlloc(tcache_bin_info, tcache_nbins, size, alignment);
        JE_ASSERT(cur_offset == size);
    }
}

}

/// jemalloc: tcache_ncached_max_compute
unsigned tcacheNcachedMaxCompute(szind_t szind)
{
    if (szind >= SC_NBINS)
        return opt.tcache_nslots_large;
    unsigned slab_nregs = bin_infos[szind].nregs;

    /// We may modify these values; start with the opt versions.
    unsigned nslots_small_min = opt.tcache_nslots_small_min;
    unsigned nslots_small_max = opt.tcache_nslots_small_max;

    /// Clamp values to meet our constraints -- even, nonzero, min < max, and suitable for a cache bin size.
    if (opt.tcache_nslots_small_max > CACHE_BIN_NCACHED_MAX)
        nslots_small_max = CACHE_BIN_NCACHED_MAX;
    if (nslots_small_min % 2 != 0)
        ++nslots_small_min;
    if (nslots_small_max % 2 != 0)
        --nslots_small_max;
    if (nslots_small_min < 2)
        nslots_small_min = 2;
    if (nslots_small_max < 2)
        nslots_small_max = 2;
    if (nslots_small_min > nslots_small_max)
        nslots_small_min = nslots_small_max;

    unsigned candidate;
    if (opt.lg_tcache_nslots_mul < 0)
        candidate = slab_nregs >> (-opt.lg_tcache_nslots_mul);
    else
        candidate = slab_nregs << opt.lg_tcache_nslots_mul;
    if (candidate % 2 != 0)
    {
        /// We need the candidate size to be even -- we assume that we can divide by two and get a positive number
        /// (e.g. when flushing).
        ++candidate;
    }
    if (candidate <= nslots_small_min)
        return nslots_small_min;
    else if (candidate <= nslots_small_max)
        return candidate;
    else
        return nslots_small_max;
}

/// jemalloc: tcache_bin_info_compute
void tcacheBinInfoCompute(CacheBinInfo * tcache_bin_info)
{
    /// Compute the values for each bin, but for bins with indices larger than tcache_nbins, no items will be cached.
    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        unsigned ncached_max = tcacheGetDefaultNcachedMaxSet(i) ? unsigned(opt.tcache_ncached_max[i]) : tcacheNcachedMaxCompute(i);
        JE_ASSERT(ncached_max <= CACHE_BIN_NCACHED_MAX);
        tcache_bin_info[i].init(static_cast<cache_bin_sz_t>(ncached_max));
    }
}

namespace
{

/// jemalloc: tcache_stack_alloc_impl
void * tcacheStackAllocImpl(ThreadState * tsdn, size_t size, size_t alignment)
{
    if (cacheBinStackUseThp())
    {
        /// Alignment is ignored since it comes from THP.
        JE_ASSERT(alignment == QUANTUM);
        return b0AllocTcacheStack(tsdn, size);
    }
    size = sz::sa2u(size, alignment);
    return ipallocztm(tsdn, size, alignment, true, nullptr, true, arenaGet(nullptr, 0, true));
}

}

constinit void * (*tcache_stack_alloc)(ThreadState * tsdn, size_t size, size_t alignment) = tcacheStackAllocImpl;

namespace
{

/// jemalloc: tsd_tcache_data_init_impl
bool tsdTcacheDataInitImpl(ThreadState & tsd, Arena * arena, const CacheBinInfo * tcache_bin_info)
{
    ThreadCacheSlow * tcache_slow = tsd.tcacheSlowGet();
    ThreadCache * tcache = tsd.tcacheGet();

    JE_ASSERT(tcache->bins[0].stillZeroInitialized());
    unsigned tcache_nbins = tcacheNbinsGet(tcache_slow);
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(tcache_bin_info, tcache_nbins, size, alignment);

    void * mem = tcache_stack_alloc(&tsd, size, alignment);
    if (mem == nullptr)
        return true;

    tcacheInit(tsd, tcache_slow, tcache, mem, tcache_bin_info);
    /// Initialization is a bit tricky here. After malloc init is done, all threads can rely on `arenaChoose` and
    /// associate the tcache accordingly. However, the thread that does the actual malloc bootstrapping relies on a
    /// functional tsd, and it can only rely on a0. In that case, we associate its tcache to a0 temporarily, and later
    /// on `arenaChooseHard` will re-associate properly.
    tcache_slow->arena = nullptr;
    if (!mallocInitialized())
    {
        /// If in initialization, assign to a0.
        arena = arenaGet(&tsd, 0, false);
        tcacheArenaAssociate(&tsd, tcache_slow, tcache, arena);
    }
    else
    {
        if (arena == nullptr)
            arena = arenaChoose(tsd, nullptr);
        /// This may happen if thread.tcache.enabled is used.
        if (tcache_slow->arena == nullptr)
            tcacheArenaAssociate(&tsd, tcache_slow, tcache, arena);
    }
    JE_ASSERT(arena == tcache_slow->arena);

    return false;
}

/// Initializes the automatic tcache (embedded in TSD). Returns true on error.
/// jemalloc: tsd_tcache_data_init
bool tsdTcacheDataInit(ThreadState & tsd, Arena * arena, const CacheBinInfo * tcache_bin_info)
{
    JE_ASSERT(tcache_bin_info != nullptr);
    bool err = tsdTcacheDataInitImpl(tsd, arena, tcache_bin_info);
    if (JE_UNLIKELY(err))
    {
        /// Disable the tcache before calling `writeMessage` to avoid recursive allocations through libc hooks.
        tsd.tcache_enabled = false;
        tsd.slowUpdate();
        writeMessage("<jemalloc>: Failed to allocate tcache data\n");
        if (opt.abort)
            abort();
    }
    return err;
}

}

/// Creates a manual tcache for the `tcache.create` mallctl.
/// jemalloc: tcache_create_explicit
ThreadCache * tcacheCreateExplicit(ThreadState & tsd)
{
    /// We place the cache bin stacks, then the `ThreadCache`, then the `ThreadCacheSlow` (whose `dyn_alloc` points
    /// to the beginning of the whole allocation, for freeing). This makes sure the cache bins have the requested
    /// alignment.
    unsigned tcache_nbins = global_do_not_change_tcache_nbins;
    size_t tcache_size;
    size_t alignment;
    cacheBinInfoComputeAlloc(tcacheGetDefaultNcachedMax(), tcache_nbins, tcache_size, alignment);

    size_t size = tcache_size + sizeof(ThreadCache) + sizeof(ThreadCacheSlow);
    /// Naturally align the pointer stacks.
    size = alignmentCeiling(size, sizeof(void *));
    size = sz::sa2u(size, alignment);

    void * mem = ipallocztm(&tsd, size, alignment, true, nullptr, true, arenaGet(nullptr, 0, true));
    if (mem == nullptr)
        return nullptr;
    ThreadCache * tcache = reinterpret_cast<ThreadCache *>(static_cast<std::byte *>(mem) + tcache_size);
    ThreadCacheSlow * tcache_slow = reinterpret_cast<ThreadCacheSlow *>(static_cast<std::byte *>(mem) + tcache_size + sizeof(ThreadCache));
    tcacheDefaultSettingsInit(tcache_slow);
    tcacheInit(tsd, tcache_slow, tcache, mem, tcacheGetDefaultNcachedMax());

    tcacheArenaAssociate(&tsd, tcache_slow, tcache, arenaIchoose(tsd, nullptr));

    return tcache;
}

/// Called upon tsd initialization.
/// jemalloc: tsd_tcache_enabled_data_init
bool tcacheTsdDataInit(ThreadState & tsd)
{
    tsd.tcache_enabled = opt.tcache;
    /// The tcache is not available yet, but we need to set up its tcache_nbins in advance.
    tcacheDefaultSettingsInit(tsd.tcacheSlowGet());
    tsd.slowUpdate();

    if (opt.tcache)
    {
        /// Trigger tcache init.
        return tsdTcacheDataInit(tsd, nullptr, tcacheGetDefaultNcachedMax());
    }

    return false;
}

/// jemalloc: tcache_enabled_set
void tcacheEnabledSet(ThreadState & tsd, bool enabled)
{
    bool was_enabled = tsd.tcache_enabled;

    if (!was_enabled && enabled)
    {
        if (tsdTcacheDataInit(tsd, nullptr, tcacheGetDefaultNcachedMax()))
            return;
    }
    else if (was_enabled && !enabled)
    {
        tcacheCleanup(tsd);
    }
    /// Commit the state last. The above calls check the current state.
    tsd.tcache_enabled = enabled;
    tsd.slowUpdate();
}

/// jemalloc: thread_tcache_max_set
bool threadTcacheMaxSet(ThreadState & tsd, size_t tcache_max)
{
    JE_ASSERT(tcache_max <= TCACHE_MAXCLASS_LIMIT);
    JE_ASSERT(tcache_max == sz::s2u(tcache_max));
    ThreadCache * tcache = tsd.tcacheGet();
    /// The slow part lives in the TSD (`tcache->tcache_slow` is null when the tcache was never initialized).
    ThreadCacheSlow * tcache_slow = tsd.tcacheSlowGet();
    CacheBinInfo tcache_bin_info[TCACHE_NBINS_MAX] = {};
    bool ret = false;
    JE_ASSERT(tcache != nullptr && tcache_slow != nullptr);

    bool enabled = tcacheAvailable(tsd);
    Arena * assigned_arena = nullptr;
    if (enabled)
    {
        assigned_arena = tcache_slow->arena;
        /// Carry over the bin settings during the reboot.
        tcacheBinSettingsBackup(tcache, tcache_bin_info);
        /// Shutdown and reboot the tcache for a clean slate.
        tcacheCleanup(tsd);
    }

    /// Still set tcache_nbins of the tcache even if the tcache is not available yet because the values are stored in
    /// the TSD and are always available for changing.
    tcacheMaxSet(tcache_slow, tcache_max);

    if (enabled)
        ret = tsdTcacheDataInit(tsd, assigned_arena, tcache_bin_info);

    JE_ASSERT(tcacheNbinsGet(tcache_slow) == sz::sizeToIndex(tcache_max) + 1);
    return ret;
}

/// jemalloc: tcache_bins_ncached_max_write
bool tcacheBinsNcachedMaxWrite(ThreadState & tsd, const char * settings, size_t len)
{
    JE_ASSERT(tcacheAvailable(tsd));
    JE_ASSERT(len != 0);
    ThreadCache * tcache = tsd.tcacheGet();
    JE_ASSERT(tcache != nullptr);
    CacheBinInfo tcache_bin_info[TCACHE_NBINS_MAX];
    tcacheBinSettingsBackup(tcache, tcache_bin_info);

    if (tcacheBinInfoSettingsParse(
            settings, len, [&](szind_t i, uint16_t ncached_max) { tcache_bin_info[i].init(ncached_max); }))
        return true;

    Arena * assigned_arena = tcache->tcache_slow->arena;
    tcacheCleanup(tsd);
    return tsdTcacheDataInit(tsd, assigned_arena, tcache_bin_info);
}

/// --- Flush and destruction -----------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tcache_flush_cache
void tcacheFlushCache(ThreadState & tsd, ThreadCache * tcache)
{
    ThreadCacheSlow * tcache_slow = tcache->tcache_slow;
    JE_ASSERT(tcache_slow->arena != nullptr);

    for (unsigned i = 0; i < tcacheNbinsGet(tcache_slow); ++i)
    {
        CacheBin * cache_bin = &tcache->bins[i];
        if (tcacheBinDisabled(i, cache_bin, tcache_slow))
            continue;
        if (i < SC_NBINS)
            tcacheBinFlushSmall(tsd, tcache, cache_bin, i, 0);
        else
            tcacheBinFlushLarge(tsd, tcache, cache_bin, i, 0);
        if constexpr (config::stats)
            JE_ASSERT(cache_bin->tstats.nrequests == 0);
    }
}

/// jemalloc: tcache_destroy
void tcacheDestroy(ThreadState & tsd, ThreadCache * tcache, bool tsd_tcache)
{
    ThreadCacheSlow * tcache_slow = tcache->tcache_slow;
    tcacheFlushCache(tsd, tcache);
    Arena * arena = tcache_slow->arena;
    tcacheArenaDissociate(&tsd, tcache_slow, tcache);

    if (tsd_tcache)
    {
        [[maybe_unused]] CacheBin * cache_bin = &tcache->bins[0];
        cache_bin->assertEmpty();
    }
    if (tsd_tcache && cacheBinStackUseThp())
        b0DallocTcacheStack(&tsd, tcache_slow->dyn_alloc);
    else
        idalloctm(&tsd, tcache_slow->dyn_alloc, nullptr, nullptr, true, true);

    /// The deallocation and tcache flush above may not trigger decay since we are on the tcache shutdown path
    /// (potentially with non-nominal tsd). Manually trigger decay to avoid pathological cases. Also include arena 0
    /// because the tcache array is allocated from it.
    arenaDecay(&tsd, arenaGet(&tsd, 0, false), false, false);

    if (arenaNthreadsGet(arena, false) == 0 && !backgroundThreadEnabled())
    {
        /// Force purging when no threads are assigned to the arena anymore.
        arenaDecay(&tsd, arena, /* is_background_thread */ false, /* all */ true);
    }
    else
    {
        arenaDecay(&tsd, arena, /* is_background_thread */ false, /* all */ false);
    }
}

}

/// jemalloc: tcache_flush
void tcacheFlush(ThreadState & tsd)
{
    JE_ASSERT(tcacheAvailable(tsd));
    tcacheFlushCache(tsd, tsd.tcacheGet());
}

/// For the automatic tcache (embedded in TSD) only.
/// jemalloc: tcache_cleanup
void tcacheCleanup(ThreadState & tsd)
{
    ThreadCache * tcache = tsd.tcacheGet();
    if (!tcacheAvailable(tsd))
    {
        JE_ASSERT(tsd.tcache_enabled == false);
        JE_ASSERT(tcache->bins[0].stillZeroInitialized());
        return;
    }
    JE_ASSERT(tsd.tcache_enabled);
    JE_ASSERT(!tcache->bins[0].stillZeroInitialized());

    tcacheDestroy(tsd, tcache, true);
    /// Make sure all bins used are reinitialized to the clean state.
    memset(static_cast<void *>(tcache->bins), 0, sizeof(CacheBin) * TCACHE_NBINS_MAX);
}

/// jemalloc: tcache_stats_merge
void tcacheStatsMerge(ThreadState * tsdn, ThreadCache * tcache, Arena * arena)
{
    static_assert(config::stats);

    /// Merge and reset tcache stats.
    for (unsigned i = 0; i < tcacheNbinsGet(tcache->tcache_slow); ++i)
    {
        CacheBin * cache_bin = &tcache->bins[i];
        if (tcacheBinDisabled(i, cache_bin, tcache->tcache_slow))
            continue;
        if (i < SC_NBINS)
        {
            Bin * bin = binChoose(tsdn, arena, i, nullptr);
            bin->lock.lock(tsdn);
            bin->stats.nrequests += cache_bin->tstats.nrequests;
            bin->lock.unlock(tsdn);
        }
        else
        {
            arenaStatsLargeFlushNrequestsAdd(tsdn, &arena->stats, i, cache_bin->tstats.nrequests);
        }
        cache_bin->tstats.nrequests = 0;
    }
}

/// --- Explicit tcaches ----------------------------------------------------------------------------------------------

namespace
{

/// Returns true on error.
/// jemalloc: tcaches_create_prep
bool tcachesCreatePrep(ThreadState & tsd, Base * base)
{
    if (tcaches == nullptr)
    {
        tcaches = static_cast<ThreadCaches *>(base->alloc(&tsd, sizeof(ThreadCache *) * (MALLOCX_TCACHE_MAX + 1), CACHELINE));
        if (tcaches == nullptr)
            return true;
    }

    if (tcaches_avail == nullptr && tcaches_past > MALLOCX_TCACHE_MAX)
        return true;

    return false;
}

/// jemalloc: tcaches_elm_remove
ThreadCache * tcachesElmRemove(ThreadState & /*tsd*/, ThreadCaches * elm, bool allow_reinit)
{
    if (elm->tcache == nullptr)
        return nullptr;
    ThreadCache * tcache = elm->tcache;
    if (allow_reinit)
        elm->tcache = TCACHES_ELM_NEED_REINIT;
    else
        elm->tcache = nullptr;

    if (tcache == TCACHES_ELM_NEED_REINIT)
        return nullptr;
    return tcache;
}

}

/// jemalloc: tcaches_create
bool tcachesCreate(ThreadState & tsd, Base * base, unsigned & r_ind)
{
    bool err;

    tcaches_mtx.lock(&tsd);

    if (tcachesCreatePrep(tsd, base))
    {
        err = true;
    }
    else if (ThreadCache * tcache = tcacheCreateExplicit(tsd); tcache == nullptr)
    {
        err = true;
    }
    else
    {
        ThreadCaches * elm;
        if (tcaches_avail != nullptr)
        {
            elm = tcaches_avail;
            tcaches_avail = tcaches_avail->next;
            elm->tcache = tcache;
            r_ind = static_cast<unsigned>(elm - tcaches);
        }
        else
        {
            elm = &tcaches[tcaches_past];
            elm->tcache = tcache;
            r_ind = tcaches_past;
            ++tcaches_past;
        }
        err = false;
    }

    tcaches_mtx.unlock(&tsd);
    return err;
}

/// jemalloc: tcaches_flush
void tcachesFlush(ThreadState & tsd, unsigned ind)
{
    tcaches_mtx.lock(&tsd);
    ThreadCache * tcache = tcachesElmRemove(tsd, &tcaches[ind], true);
    tcaches_mtx.unlock(&tsd);
    if (tcache != nullptr)
    {
        /// Destroy the tcache; recreate in `tcachesGet` if needed.
        tcacheDestroy(tsd, tcache, false);
    }
}

/// jemalloc: tcaches_destroy
void tcachesDestroy(ThreadState & tsd, unsigned ind)
{
    tcaches_mtx.lock(&tsd);
    ThreadCaches * elm = &tcaches[ind];
    ThreadCache * tcache = tcachesElmRemove(tsd, elm, false);
    elm->next = tcaches_avail;
    tcaches_avail = elm;
    tcaches_mtx.unlock(&tsd);
    if (tcache != nullptr)
        tcacheDestroy(tsd, tcache, false);
}

/// --- Boot, fork ----------------------------------------------------------------------------------------------------

/// jemalloc: tcache_boot
bool tcacheBoot(ThreadState * /*tsdn*/, Base * /*base*/)
{
    global_do_not_change_tcache_maxclass = sz::s2u(opt.tcache_max);
    JE_ASSERT(global_do_not_change_tcache_maxclass <= TCACHE_MAXCLASS_LIMIT);
    global_do_not_change_tcache_nbins = sz::sizeToIndex(global_do_not_change_tcache_maxclass) + 1;
    /// Pre-compute the default bin info. After this, it should not be modified and should always be accessed using
    /// `tcacheGetDefaultNcachedMax`.
    tcacheBinInfoCompute(tcache_default_ncached_max);

    if (tcaches_mtx.init("tcaches", MutexRank::TCACHES, MutexLockOrder::RankExclusive))
        return true;

    return false;
}

/// jemalloc: tcache_prefork
void tcachePrefork(ThreadState * tsdn)
{
    tcaches_mtx.prefork(tsdn);
}

/// jemalloc: tcache_postfork_parent
void tcachePostforkParent(ThreadState * tsdn)
{
    tcaches_mtx.postforkParent(tsdn);
}

/// jemalloc: tcache_postfork_child
void tcachePostforkChild(ThreadState * tsdn)
{
    tcaches_mtx.postforkChild(tsdn);
}

/// jemalloc: tcache_assert_initialized
void tcacheAssertInitialized([[maybe_unused]] ThreadCache * tcache)
{
    JE_ASSERT(!tcache->bins[0].stillZeroInitialized());
}

}
