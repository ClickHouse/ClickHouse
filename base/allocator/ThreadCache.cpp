#include <allocator/ThreadCache.h>

#include <allocator/ArenaInlines.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Frontend.h>
#include <allocator/MallocConf.h>
#include <allocator/Mutex.h>
#include <allocator/ThreadEvent.h>

namespace jemalloc
{

using namespace thread_cache_detail;

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit unsigned global_do_not_change_thread_cache_num_bins = 0;
constinit size_t global_do_not_change_thread_cache_max_class = 0;
constinit ThreadCaches * explicit_thread_caches = nullptr;

namespace
{

/// Default bin info for each bin: initialized from `opt.tcache_ncached_max` (malloc_conf) and the computed defaults
/// in `threadCacheBoot`; not modified after that.
/// jemalloc: opt_tcache_ncached_max (after `tcache_boot`)
constinit CacheBinInfo thread_cache_default_num_cached_max[THREAD_CACHE_NUM_BINS_MAX] = {};

/// Index of the first element within `explicit_thread_caches` that has never been used.
/// jemalloc: tcaches_past
constinit unsigned explicit_thread_caches_past = 0;

/// Head of the singly linked list tracking available `explicit_thread_caches` elements.
/// jemalloc: tcaches_avail
constinit ThreadCaches * explicit_thread_caches_available = nullptr;

/// Protects `explicit_thread_caches`, `explicit_thread_caches_past`, `explicit_thread_caches_available`.
/// jemalloc: tcaches_mtx (WITNESS_RANK_TCACHES)
constinit Mutex explicit_thread_caches_mutex;

}

/// jemalloc: tcache_salloc
size_t threadCacheAllocationSize(ThreadState * thread_state, const void * ptr)
{
    return arenaAllocationSize(thread_state, ptr);
}

/// --- GC event wait functions ---------------------------------------------------------------------------------------

/// jemalloc: tcache_gc_new_event_wait
uint64_t threadCacheGCNewEventWait(ThreadState & /*tsd*/)
{
    return options.thread_cache_gc_increment_bytes;
}

/// jemalloc: tcache_gc_postponed_event_wait
uint64_t threadCacheGCPostponedEventWait(ThreadState & /*tsd*/)
{
    return THREAD_EVENT_MIN_START_WAIT;
}

/// `tcache_gc_dalloc_new_event_wait` / `tcache_gc_dalloc_postponed_event_wait` are dead in jemalloc (the same handler
/// is registered for alloc and dalloc) and are not ported.

/// --- Fill count control ---------------------------------------------------------------------------------------------

namespace thread_cache_detail
{

/// jemalloc: tcache_bin_fill_ctl_init
void threadCacheBinFillControlInit(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_BINS);
    CacheBinFillControl * mallctl = &thread_cache_slow->bin_fill_control_do_not_access_directly[size_class_idx];
    mallctl->base = 1;
    mallctl->offset = 0;
}

/// jemalloc: tcache_bin_fill_ctl_get
CacheBinFillControl * threadCacheBinFillControlGet(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_BINS);
    CacheBinFillControl * mallctl = &thread_cache_slow->bin_fill_control_do_not_access_directly[size_class_idx];
    ALLOCATOR_ASSERT(mallctl->base > mallctl->offset);
    return mallctl;
}

/// The number of items to be filled at a time for a given small bin is `num_cached_max >> lg_fill_div`, where
/// `lg_fill_div = base - offset`. The base is adjusted during GC based on the traffic within a period of time, while
/// the offset is updated in real time to handle the immediate traffic.
/// jemalloc: tcache_nfill_small_lg_div_get
uint8_t threadCacheNumFillSmallLog2DivisionGet(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    CacheBinFillControl * mallctl = threadCacheBinFillControlGet(thread_cache_slow, size_class_idx);
    return static_cast<uint8_t>(mallctl->base - (options.experimental_thread_cache_gc ? mallctl->offset : 0));
}

/// When we want to fill more items to respond to burst load, the offset is increased so that (base - offset)
/// decreases, which in turn increases the number of items to be filled.
/// jemalloc: tcache_nfill_small_burst_prepare
void threadCacheNumFillSmallBurstPrepare(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    CacheBinFillControl * mallctl = threadCacheBinFillControlGet(thread_cache_slow, size_class_idx);
    if (mallctl->offset + 1 < mallctl->base)
        ++mallctl->offset;
}

/// jemalloc: tcache_nfill_small_burst_reset
void threadCacheNumFillSmallBurstReset(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    CacheBinFillControl * mallctl = threadCacheBinFillControlGet(thread_cache_slow, size_class_idx);
    mallctl->offset = 0;
}

/// limit == 0: the fill count should be increased, i.e. lg_div (base) should be decreased.
/// limit != 0: limit is ncached_max, the fill count should be decreased, i.e. lg_div (base) should be increased.
/// jemalloc: tcache_nfill_small_gc_update
void threadCacheNumFillSmallGCUpdate(ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx, CacheBinSize limit)
{
    CacheBinFillControl * mallctl = threadCacheBinFillControlGet(thread_cache_slow, size_class_idx);
    if (!limit && mallctl->base > 1)
    {
        /// Increase fill count by 2X for small bins. Make sure lg_fill_div stays greater than 1.
        --mallctl->base;
    }
    else if (limit && (limit >> mallctl->base) > 1)
    {
        /// Reduce fill count by 2X. Limit lg_fill_div such that the fill count is always at least 1.
        ++mallctl->base;
    }
    /// Reset the offset for the next GC period.
    mallctl->offset = 0;
}

/// jemalloc: tcache_gc_item_delay_compute
uint8_t threadCacheGCItemDelayCompute(SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_BINS);
    size_t size = size_classes::indexToSize(size_class_idx);
    size_t item_delay = options.thread_cache_gc_delay_bytes / size;
    size_t delay_max = size_t(1) << (sizeof(ThreadCacheSlow::bin_flush_delay_items[0]) * 8);
    if (item_delay >= delay_max)
        item_delay = delay_max - 1;
    return static_cast<uint8_t>(item_delay);
}

/// --- GC ------------------------------------------------------------------------------------------------------------

/// jemalloc: tcache_gc_is_addr_remote
bool threadCacheGCIsAddrRemote(void * addr, uintptr_t min, uintptr_t max)
{
    ALLOCATOR_ASSERT(addr != nullptr);
    return reinterpret_cast<uintptr_t>(addr) < min || reinterpret_cast<uintptr_t>(addr) >= max;
}

/// Counts the cached pointers that are remote w.r.t. the slab at `addr` or its 2 MiB neighborhood, and selects the
/// range to keep.
/// jemalloc: tcache_gc_small_nremote_get
CacheBinSize threadCacheGCSmallNumRemoteGet(
    CacheBin * cache_bin, void * addr, uintptr_t & addr_min, uintptr_t & addr_max, SizeClassIdx size_class_idx, size_t num_flush)
{
    ALLOCATOR_ASSERT(addr != nullptr);
    /// The slab address range that the provided addr belongs to.
    uintptr_t slab_min = reinterpret_cast<uintptr_t>(addr);
    uintptr_t slab_max = slab_min + bin_infos[size_class_idx].slab_size;
    /// When growing retained virtual memory, it's increased exponentially, starting from 2M, so that the total number
    /// of disjoint virtual memory ranges retained by each shard is limited.
    uintptr_t neighbor_min = (reinterpret_cast<uintptr_t>(addr) > THREAD_CACHE_GC_NEIGHBOR_LIMIT)
        ? (reinterpret_cast<uintptr_t>(addr) - THREAD_CACHE_GC_NEIGHBOR_LIMIT)
        : 0;
    uintptr_t neighbor_max = (reinterpret_cast<uintptr_t>(addr) < (UINTPTR_MAX - THREAD_CACHE_GC_NEIGHBOR_LIMIT))
        ? (reinterpret_cast<uintptr_t>(addr) + THREAD_CACHE_GC_NEIGHBOR_LIMIT)
        : UINTPTR_MAX;

    /// Scan the entire bin to count the number of remote pointers.
    void ** head = cache_bin->stack_head;
    CacheBinSize num_remote_slab = 0;
    CacheBinSize num_remote_neighbor = 0;
    CacheBinSize num_cached = cache_bin->numCachedGetLocal();
    for (void ** current = head; current < head + num_cached; ++current)
    {
        num_remote_slab = static_cast<CacheBinSize>(num_remote_slab + threadCacheGCIsAddrRemote(*current, slab_min, slab_max));
        num_remote_neighbor
            = static_cast<CacheBinSize>(num_remote_neighbor + threadCacheGCIsAddrRemote(*current, neighbor_min, neighbor_max));
    }
    /// Since the slab size is dynamic and can be larger than 2M (`THREAD_CACHE_GC_NEIGHBOR_LIMIT`), there is no guarantee
    /// as to which of `num_remote_slab` and `num_remote_neighbor` is greater.
    ALLOCATOR_ASSERT(num_remote_slab <= num_cached && num_remote_neighbor <= num_cached);
    /// We first consider keeping ptrs from the neighboring addr range, since in most cases the range is greater than
    /// the slab range. So if the number of non-neighbor ptrs is more than the intended flush amount, we use it as the
    /// anchor for flushing.
    if (num_remote_neighbor >= num_flush)
    {
        addr_min = neighbor_min;
        addr_max = neighbor_max;
        return num_remote_neighbor;
    }
    /// We then consider only keeping ptrs from the local slab, and in most cases this is stricter, assuming that
    /// slab < 2M is the common case.
    addr_min = slab_min;
    addr_max = slab_max;
    return num_remote_slab;
}

/// Shuffles the ptrs in the bin to put the remote pointers at the bottom; the local ones move to the top keeping
/// their relative order.
/// jemalloc: tcache_gc_small_bin_shuffle
void threadCacheGCSmallBinShuffle(CacheBin * cache_bin, CacheBinSize num_remote, uintptr_t addr_min, uintptr_t addr_max)
{
    void ** swap = nullptr;
    CacheBinSize num_cached = cache_bin->numCachedGetLocal();
    CacheBinSize num_top = static_cast<CacheBinSize>(num_cached - num_remote);
    CacheBinSize count = 0;
    ALLOCATOR_ASSERT(num_top > 0 && num_top < num_cached);
    /// Scan the [head, head + ntop) part of the cache bin, bubbling the non-remote ptrs to the top of the bin. After
    /// this, [head, head + cnt) contains only non-remote ptrs, in the same relative order as before, while
    /// [head + cnt, head + ntop) contains only remote ptrs.
    void ** head = cache_bin->stack_head;
    for (void ** current = head; current < head + num_top; ++current)
    {
        if (!threadCacheGCIsAddrRemote(*current, addr_min, addr_max))
        {
            /// Tracks the number of non-remote ptrs seen so far.
            ++count;
            /// There is a remote ptr before the current non-remote ptr: swap them, and increment the swap pointer so
            /// that it still points to the top remote ptr in the bin.
            if (swap != nullptr)
            {
                ALLOCATOR_ASSERT(swap < current);
                ALLOCATOR_ASSERT(threadCacheGCIsAddrRemote(*swap, addr_min, addr_max));
                void * tmp = *current;
                *current = *swap;
                *swap = tmp;
                ++swap;
                ALLOCATOR_ASSERT(swap <= current);
                ALLOCATOR_ASSERT(threadCacheGCIsAddrRemote(*swap, addr_min, addr_max));
            }
            continue;
        }
        else if (swap == nullptr)
        {
            /// Swap always points to the top remote ptr in the bin.
            swap = current;
        }
    }
    /// Scan the [head + ntop, head + ncached) part of the cache bin, after which it should only contain remote ptrs.
    for (void ** current = head + num_top; current < head + num_cached; ++current)
    {
        /// Early break if all non-remote ptrs have been moved.
        if (count == num_top)
            break;
        if (!threadCacheGCIsAddrRemote(*current, addr_min, addr_max))
        {
            ALLOCATOR_ASSERT(threadCacheGCIsAddrRemote(*(head + count), addr_min, addr_max));
            void * tmp = *current;
            *current = *(head + count);
            *(head + count) = tmp;
            ++count;
        }
    }
    ALLOCATOR_ASSERT(count == num_top);
    /// Sanity check to make sure the shuffle is done correctly.
    if constexpr (config::debug)
    {
        for (void ** current = head; current < head + num_cached; ++current)
        {
            ALLOCATOR_ASSERT(*current != nullptr);
            ALLOCATOR_ASSERT(
                ((current < head + num_top) && !threadCacheGCIsAddrRemote(*current, addr_min, addr_max))
                || ((current >= head + num_top) && threadCacheGCIsAddrRemote(*current, addr_min, addr_max)));
        }
    }
}

}

namespace
{

/// The base address of the arena's current slab of the bin (`current_slab`, else the first nonfull slab), or null.
/// jemalloc: tcache_gc_small_heuristic_addr_get
inline void *
threadCacheGCSmallHeuristicAddrGet(ThreadState & thread_state, ThreadCacheSlow * thread_cache_slow, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_BINS);
    ThreadState * thread_state_ptr = &thread_state;
    Bin * bin = binChoose(thread_state_ptr, thread_cache_slow->arena, size_class_idx, nullptr);
    ALLOCATOR_ASSERT(bin != nullptr);

    bin->lock.lock(thread_state_ptr);
    Extent * slab = (bin->current_slab == nullptr) ? bin->slabs_non_full.first() : bin->current_slab;
    ALLOCATOR_ASSERT(slab != nullptr || bin->slabs_non_full.empty());
    void * result = (slab != nullptr) ? slab->addr() : nullptr;
    ALLOCATOR_ASSERT(result != nullptr || slab == nullptr);
    bin->lock.unlock(thread_state_ptr);

    return result;
}

/// Aims to flush 3/4 of the items below low-water, with remote pointers being prioritized for flushing.
/// jemalloc: tcache_gc_small
bool threadCacheGCSmall(
    ThreadState & thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_BINS);

    CacheBin * cache_bin = &thread_cache->bins[size_class_idx];
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(size_class_idx, cache_bin, thread_cache->thread_cache_slow));
    CacheBinSize num_cached = cache_bin->numCachedGetLocal();
    CacheBinSize low_water = cache_bin->lowWaterGet();
    if (low_water > 0)
    {
        /// There are unused items within the GC period => reduce the fill count. The limit != 0 is borrowed to
        /// indicate that the fill count should be reduced.
        threadCacheNumFillSmallGCUpdate(thread_cache_slow, size_class_idx, /* limit */ cache_bin->numCachedMaxGet());
    }
    else if (thread_cache_slow->bin_refilled[size_class_idx])
    {
        /// There have been refills within the GC period => increase the fill count. The limit set to 0 is borrowed
        /// to indicate that the fill count should be increased.
        threadCacheNumFillSmallGCUpdate(thread_cache_slow, size_class_idx, /* limit */ 0);
        thread_cache_slow->bin_refilled[size_class_idx] = false;
    }
    ALLOCATOR_ASSERT(!thread_cache_slow->bin_refilled[size_class_idx]);

    CacheBinSize num_flush = static_cast<CacheBinSize>(low_water - (low_water >> 2));
    /// When the new tcache gc is not enabled, keep the flush delay logic, and directly flush the bottom nflush items
    /// if needed.
    if (!options.experimental_thread_cache_gc)
    {
        if (num_flush < thread_cache_slow->bin_flush_delay_items[size_class_idx])
        {
            uint8_t num_flush_uint8 = static_cast<uint8_t>(num_flush);
            thread_cache_slow->bin_flush_delay_items[size_class_idx]
                = static_cast<uint8_t>(thread_cache_slow->bin_flush_delay_items[size_class_idx] - num_flush_uint8);
            return false;
        }

        thread_cache_slow->bin_flush_delay_items[size_class_idx] = threadCacheGCItemDelayCompute(size_class_idx);
        goto label_flush;
    }

    {
        /// Directly go to the flush path when the entire bin needs to be flushed.
        if (num_flush == num_cached)
            goto label_flush;

        /// Query the arena binshard to get heuristic locality info.
        void * addr = threadCacheGCSmallHeuristicAddrGet(thread_state, thread_cache_slow, size_class_idx);
        if (addr == nullptr)
            goto label_flush;

        /// Use the queried addr above to get the number of remote ptrs in the bin, and the min/max of the local addr
        /// range.
        uintptr_t addr_min;
        uintptr_t addr_max;
        CacheBinSize num_remote = threadCacheGCSmallNumRemoteGet(cache_bin, addr, addr_min, addr_max, size_class_idx, num_flush);

        /// Update nflush to the larger of the intended flush count and the number of remote ptrs.
        if (num_remote > num_flush)
            num_flush = num_remote;
        /// When entering the locality check, nflush should be less than ncached, otherwise the entire bin should be
        /// flushed regardless. The only case when nflush gets updated to ncached after the locality check is when
        /// all the items in the bin are remote, in which case the entire bin should also be flushed.
        ALLOCATOR_ASSERT(num_flush < num_cached || num_remote == num_cached);
        if (num_remote == 0 || num_remote == num_cached)
            goto label_flush;

        /// Move the remote pointers to the bottom of the bin for flushing. As long as moved to the bottom, the order
        /// of these nremote ptrs does not matter, since they are going to be flushed anyway. The rest of the ptrs are
        /// moved to the top of the bin, and their relative order is maintained.
        threadCacheGCSmallBinShuffle(cache_bin, num_remote, addr_min, addr_max);
    }

label_flush:
    if (num_flush == 0)
    {
        ALLOCATOR_ASSERT(low_water == 0);
        return false;
    }
    ALLOCATOR_ASSERT(num_flush <= num_cached);
    threadCacheBinFlushSmall(thread_state, thread_cache, cache_bin, size_class_idx, static_cast<unsigned>(num_cached - num_flush));
    return true;
}

/// Like the small GC, flushes 3/4 of the untouched items; but simply the bottom ones, without any locality check.
/// jemalloc: tcache_gc_large
bool threadCacheGCLarge(
    ThreadState & thread_state, ThreadCacheSlow * /*tcache_slow*/, ThreadCache * thread_cache, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(size_class_idx >= SIZE_CLASS_NUM_BINS);
    CacheBin * cache_bin = &thread_cache->bins[size_class_idx];
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(size_class_idx, cache_bin, thread_cache->thread_cache_slow));
    CacheBinSize low_water = cache_bin->lowWaterGet();
    if (low_water == 0)
        return false;
    unsigned num_remaining = static_cast<unsigned>(cache_bin->numCachedGetLocal() - low_water + (low_water >> 2));
    threadCacheBinFlushLarge(thread_state, thread_cache, cache_bin, size_class_idx, num_remaining);
    return true;
}

/// Tries to GC one bin; returns true if some items were flushed.
/// jemalloc: tcache_try_gc_bin
bool threadCacheTryGCBin(
    ThreadState & thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, SizeClassIdx size_class_idx)
{
    ALLOCATOR_ASSERT(thread_cache != nullptr);
    CacheBin * cache_bin = &thread_cache->bins[size_class_idx];
    if (threadCacheBinDisabled(size_class_idx, cache_bin, thread_cache_slow))
        return false;

    bool is_small = (size_class_idx < SIZE_CLASS_NUM_BINS);
    threadCacheBinFlushStashed(thread_state, thread_cache, cache_bin, size_class_idx, is_small);
    bool result = is_small ? threadCacheGCSmall(thread_state, thread_cache_slow, thread_cache, size_class_idx)
                           : threadCacheGCLarge(thread_state, thread_cache_slow, thread_cache, size_class_idx);
    cache_bin->lowWaterSet();
    return result;
}

}

/// jemalloc: tcache_gc_event
void threadCacheGCEvent(ThreadState & thread_state)
{
    ThreadCache * thread_cache = threadCacheGet(thread_state);
    if (thread_cache == nullptr)
        return;

    ThreadCacheSlow * thread_cache_slow = thread_state.threadCacheSlowGet();
    ALLOCATOR_ASSERT(thread_cache_slow != nullptr);

    /// When the new tcache gc is not enabled, GC one bin at a time.
    if (!options.experimental_thread_cache_gc)
    {
        SizeClassIdx size_class_idx = thread_cache_slow->next_gc_bin;
        threadCacheTryGCBin(thread_state, thread_cache_slow, thread_cache, size_class_idx);
        ++thread_cache_slow->next_gc_bin;
        if (thread_cache_slow->next_gc_bin == threadCacheNumBinsGet(thread_cache_slow))
            thread_cache_slow->next_gc_bin = 0;
        return;
    }

    Nanoseconds now = thread_cache_slow->last_gc_time;
    now.update();
    ALLOCATOR_ASSERT(now.compare(thread_cache_slow->last_gc_time) >= 0);

    if (now.ns() - thread_cache_slow->last_gc_time.ns() < THREAD_CACHE_GC_INTERVAL_NS)
    {
        /// The time interval is too short, skip this event.
        return;
    }
    /// Update last_gc_time to now.
    thread_cache_slow->last_gc_time = now;

    unsigned gc_small_num_bins = 0;
    unsigned gc_large_num_bins = 0;
    unsigned thread_cache_num_bins = threadCacheNumBinsGet(thread_cache_slow);
    unsigned small_num_bins = thread_cache_num_bins > SIZE_CLASS_NUM_BINS ? SIZE_CLASS_NUM_BINS : thread_cache_num_bins;
    SizeClassIdx size_class_idx_small = thread_cache_slow->next_gc_bin_small;
    SizeClassIdx size_class_idx_large = thread_cache_slow->next_gc_bin_large;

    /// Flush at most `THREAD_CACHE_GC_SMALL_NUM_BINS_MAX` small bins at a time.
    for (unsigned i = 0; i < small_num_bins && gc_small_num_bins < THREAD_CACHE_GC_SMALL_NUM_BINS_MAX; ++i)
    {
        ALLOCATOR_ASSERT(size_class_idx_small < SIZE_CLASS_NUM_BINS);
        if (threadCacheTryGCBin(thread_state, thread_cache_slow, thread_cache, size_class_idx_small))
            ++gc_small_num_bins;
        if (++size_class_idx_small == small_num_bins)
            size_class_idx_small = 0;
    }
    thread_cache_slow->next_gc_bin_small = size_class_idx_small;

    if (thread_cache_num_bins <= SIZE_CLASS_NUM_BINS)
        return;

    /// Flush at most `THREAD_CACHE_GC_LARGE_NUM_BINS_MAX` large bins at a time.
    for (unsigned i = SIZE_CLASS_NUM_BINS; i < thread_cache_num_bins && gc_large_num_bins < THREAD_CACHE_GC_LARGE_NUM_BINS_MAX; ++i)
    {
        ALLOCATOR_ASSERT(size_class_idx_large >= SIZE_CLASS_NUM_BINS && size_class_idx_large < thread_cache_num_bins);
        if (threadCacheTryGCBin(thread_state, thread_cache_slow, thread_cache, size_class_idx_large))
            ++gc_large_num_bins;
        if (++size_class_idx_large == thread_cache_num_bins)
            size_class_idx_large = SIZE_CLASS_NUM_BINS;
    }
    thread_cache_slow->next_gc_bin_large = size_class_idx_large;
}

/// --- Fill and flush ------------------------------------------------------------------------------------------------

/// jemalloc: tcache_alloc_small_hard
void * threadCacheAllocSmallHard(
    ThreadState * thread_state,
    Arena * arena,
    ThreadCache * thread_cache,
    CacheBin * cache_bin,
    SizeClassIdx bin_idx,
    bool & thread_cache_success)
{
    ThreadCacheSlow * thread_cache_slow = thread_cache->thread_cache_slow;
    void * result;

    ALLOCATOR_ASSERT(thread_cache_slow->arena != nullptr);
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(bin_idx, cache_bin, thread_cache_slow));
    ALLOCATOR_ASSERT(cache_bin->numCachedGetLocal() == 0);
    CacheBinSize num_fill
        = static_cast<CacheBinSize>(cache_bin->numCachedMaxGet() >> threadCacheNumFillSmallLog2DivisionGet(thread_cache_slow, bin_idx));
    if (num_fill == 0)
        num_fill = 1;
    CacheBinSize num_fill_min = options.experimental_thread_cache_gc ? static_cast<CacheBinSize>((num_fill >> 1) + 1) : num_fill;
    CacheBinSize num_fill_max = num_fill;
    CacheBinPtrArray ptrs(num_fill_max);
    cache_bin->initPtrArrayForFill(ptrs, num_fill_max);

    CacheBinSize filled = arenaPtrArrayFillSmall(
        thread_state, arena, bin_idx, &ptrs, /* nfill_min */ num_fill_min, /* nfill_max */ num_fill_max, cache_bin->thread_cache_stats);
    cache_bin->finishFill(ptrs, filled);
    ALLOCATOR_ASSERT(filled >= num_fill_min && filled <= num_fill_max);
    ALLOCATOR_ASSERT(cache_bin->numCachedGetLocal() == filled);

    thread_cache_slow->bin_refilled[bin_idx] = true;
    threadCacheNumFillSmallBurstPrepare(thread_cache_slow, bin_idx);
    result = cache_bin->alloc(thread_cache_success);

    return result;
}

namespace
{

/// jemalloc: tcache_bin_flush_bottom
ALLOCATOR_ALWAYS_INLINE void threadCacheBinFlushBottom(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, unsigned remainder, bool small)
{
    ALLOCATOR_ASSERT(remainder <= cache_bin->numCachedMaxGet());
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(bin_idx, cache_bin, thread_cache->thread_cache_slow));
    [[maybe_unused]] CacheBinSize original_num_stashed = cache_bin->numStashedGetLocal();
    threadCacheBinFlushStashed(thread_state, thread_cache, cache_bin, bin_idx, small);

    CacheBinSize num_cached = cache_bin->numCachedGetLocal();
    ALLOCATOR_ASSERT(static_cast<CacheBinSize>(remainder) <= num_cached + original_num_stashed);
    if (static_cast<CacheBinSize>(remainder) > num_cached)
    {
        /// The stashed flush above could have done enough flushing, if there were many items stashed. Validate
        /// that: 1) non zero stashed, and 2) the bin stack has available space now.
        ALLOCATOR_ASSERT(original_num_stashed > 0);
        ALLOCATOR_ASSERT(num_cached + cache_bin->numStashedGetLocal() < cache_bin->numCachedMaxGet());
        /// Still go through the flush logic for stats purpose only.
        remainder = num_cached;
    }
    CacheBinSize num_flush = static_cast<CacheBinSize>(num_cached - static_cast<CacheBinSize>(remainder));

    CacheBinPtrArray ptrs(num_flush);
    cache_bin->initPtrArrayForFlush(ptrs, num_flush);

    arenaPtrArrayFlush(
        thread_state, bin_idx, &ptrs, num_flush, small, thread_cache->thread_cache_slow->arena, cache_bin->thread_cache_stats);

    cache_bin->finishFlush(ptrs, num_flush);
}

}

/// jemalloc: tcache_bin_flush_small
void threadCacheBinFlushSmall(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, unsigned remainder)
{
    threadCacheNumFillSmallBurstReset(thread_cache->thread_cache_slow, bin_idx);
    threadCacheBinFlushBottom(thread_state, thread_cache, cache_bin, bin_idx, remainder, /* small */ true);
}

/// jemalloc: tcache_bin_flush_large
void threadCacheBinFlushLarge(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, unsigned remainder)
{
    threadCacheBinFlushBottom(thread_state, thread_cache, cache_bin, bin_idx, remainder, /* small */ false);
}

/// Flushing stashed happens on 1) tcache fill, 2) tcache flush, or 3) tcache GC event. This makes sure that the
/// stashed items do not hold memory for too long, and new buffers can only be allocated when nothing is stashed.
///
/// The downside is, the time between stash and flush may be relatively short, especially when the request rate is
/// high. It lowers the chance of detecting write-after-free -- however that is a delayed detection anyway, and is less
/// of a focus than the memory overhead.
/// jemalloc: tcache_bin_flush_stashed
void threadCacheBinFlushStashed(
    ThreadState & thread_state, ThreadCache * thread_cache, CacheBin * cache_bin, SizeClassIdx bin_idx, bool is_small)
{
    ALLOCATOR_ASSERT(!threadCacheBinDisabled(bin_idx, cache_bin, thread_cache->thread_cache_slow));
    /// The two below are for assertion only. The content of the original cached items remains unchanged -- the
    /// stashed items reside on the other end of the stack. Checking the stack head and ncached to verify.
    [[maybe_unused]] void * head_content = *cache_bin->stack_head;
    [[maybe_unused]] CacheBinSize original_cached = cache_bin->numCachedGetLocal();

    CacheBinSize num_stashed = cache_bin->numStashedGetLocal();
    ALLOCATOR_ASSERT(original_cached + num_stashed <= cache_bin->numCachedMaxGet());
    if (num_stashed == 0)
        return;

    CacheBinPtrArray ptrs(num_stashed);
    cache_bin->initPtrArrayForStashed(bin_idx, ptrs, num_stashed);
    sanitizerCheckStashedPtrs(ptrs.ptr, num_stashed, size_classes::indexToSize(bin_idx));
    arenaPtrArrayFlush(
        thread_state, bin_idx, &ptrs, num_stashed, is_small, thread_cache->thread_cache_slow->arena, cache_bin->thread_cache_stats);
    cache_bin->finishFlushStashed();

    ALLOCATOR_ASSERT(cache_bin->numStashedGetLocal() == 0);
    ALLOCATOR_ASSERT(cache_bin->numCachedGetLocal() == original_cached);
    ALLOCATOR_ASSERT(head_content == *cache_bin->stack_head);
}

/// --- Bin settings --------------------------------------------------------------------------------------------------

/// jemalloc: tcache_get_default_ncached_max_set
bool threadCacheGetDefaultNumCachedMaxSet(SizeClassIdx idx)
{
    return options.thread_cache_num_cached_max_set[idx];
}

/// jemalloc: tcache_get_default_ncached_max
const CacheBinInfo * threadCacheGetDefaultNumCachedMax()
{
    return thread_cache_default_num_cached_max;
}

/// jemalloc: tcache_bin_ncached_max_read
bool threadCacheBinNumCachedMaxRead(ThreadState & thread_state, size_t bin_size, CacheBinSize & num_cached_max)
{
    if (bin_size > THREAD_CACHE_MAX_CLASS_LIMIT)
        return true;

    if (!threadCacheAvailable(thread_state))
    {
        num_cached_max = 0;
        return false;
    }

    ThreadCache * thread_cache = thread_state.threadCacheGet();
    ALLOCATOR_ASSERT(thread_cache != nullptr);
    SizeClassIdx bin_idx = size_classes::sizeToIndex(bin_size);

    CacheBin * bin = &thread_cache->bins[bin_idx];
    num_cached_max = threadCacheBinDisabled(bin_idx, bin, thread_cache->thread_cache_slow) ? 0 : bin->numCachedMaxGet();
    return false;
}

/// --- Arena association ---------------------------------------------------------------------------------------------

/// jemalloc: tcache_arena_associate
void threadCacheArenaAssociate(ThreadState * thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, Arena * arena)
{
    ALLOCATOR_ASSERT(thread_cache_slow->arena == nullptr);
    thread_cache_slow->arena = arena;

    if constexpr (config::stats)
    {
        /// Link into the list of extant tcaches.
        arena->thread_cache_list_mutex.lock(thread_state);

        arena->thread_cache_list.elementInit(thread_cache_slow);
        arena->thread_cache_list.tailInsert(thread_cache_slow);
        thread_cache_slow->cache_bin_array_descriptor.init(thread_cache->bins);
        arena->cache_bin_array_descriptor_list.tailInsert(&thread_cache_slow->cache_bin_array_descriptor);

        arena->thread_cache_list_mutex.unlock(thread_state);
    }
}

namespace
{

/// jemalloc: tcache_arena_dissociate
void threadCacheArenaDissociate(ThreadState * thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * /*tcache*/)
{
    Arena * arena = thread_cache_slow->arena;
    ALLOCATOR_ASSERT(arena != nullptr);
    if constexpr (config::stats)
    {
        /// Unlink from the list of extant tcaches.
        arena->thread_cache_list_mutex.lock(thread_state);
        if constexpr (config::debug)
        {
            bool in_list = false;
            arena->thread_cache_list.forEach(
                [&](ThreadCacheSlow * iterate)
                {
                    if (iterate == thread_cache_slow)
                        in_list = true;
                });
            ALLOCATOR_ASSERT(in_list);
        }
        arena->thread_cache_list.remove(thread_cache_slow);
        arena->cache_bin_array_descriptor_list.remove(&thread_cache_slow->cache_bin_array_descriptor);
        threadCacheStatsMerge(thread_state, thread_cache_slow->thread_cache, arena);
        arena->thread_cache_list_mutex.unlock(thread_state);
    }
    thread_cache_slow->arena = nullptr;
}

}

/// jemalloc: tcache_arena_reassociate
void threadCacheArenaReassociate(ThreadState * thread_state, ThreadCacheSlow * thread_cache_slow, ThreadCache * thread_cache, Arena * arena)
{
    threadCacheArenaDissociate(thread_state, thread_cache_slow, thread_cache);
    threadCacheArenaAssociate(thread_state, thread_cache_slow, thread_cache, arena);
}

/// --- Initialization ------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tcache_default_settings_init
void threadCacheDefaultSettingsInit(ThreadCacheSlow * thread_cache_slow)
{
    ALLOCATOR_ASSERT(thread_cache_slow != nullptr);
    ALLOCATOR_ASSERT(global_do_not_change_thread_cache_max_class != 0);
    ALLOCATOR_ASSERT(global_do_not_change_thread_cache_num_bins != 0);
    thread_cache_slow->thread_cache_num_bins = global_do_not_change_thread_cache_num_bins;
}

/// jemalloc: tcache_init
void threadCacheInit(
    ThreadState & /*tsd*/,
    ThreadCacheSlow * thread_cache_slow,
    ThreadCache * thread_cache,
    void * memory,
    const CacheBinInfo * thread_cache_bin_info)
{
    thread_cache->thread_cache_slow = thread_cache_slow;
    thread_cache_slow->thread_cache = thread_cache;

    thread_cache_slow->link = {};
    thread_cache_slow->last_gc_time = Nanoseconds::zero();
    thread_cache_slow->next_gc_bin = 0;
    thread_cache_slow->next_gc_bin_small = 0;
    thread_cache_slow->next_gc_bin_large = SIZE_CLASS_NUM_BINS;
    thread_cache_slow->arena = nullptr;
    thread_cache_slow->dynamic_alloc = memory;

    /// We reserve cache bins for all small size classes, even if some may not get used (i.e. bins higher than
    /// tcache_nbins). This allows the fast and common paths to access cache bin metadata safely w/o worrying about
    /// which ones are disabled.
    unsigned thread_cache_num_bins = threadCacheNumBinsGet(thread_cache_slow);
    size_t current_offset = 0;
    cacheBinPreincrement(thread_cache_bin_info, thread_cache_num_bins, memory, current_offset);
    for (unsigned i = 0; i < thread_cache_num_bins; ++i)
    {
        if (i < SIZE_CLASS_NUM_BINS)
        {
            threadCacheBinFillControlInit(thread_cache_slow, i);
            thread_cache_slow->bin_refilled[i] = false;
            thread_cache_slow->bin_flush_delay_items[i] = threadCacheGCItemDelayCompute(i);
        }
        CacheBin * cache_bin = &thread_cache->bins[i];
        if (thread_cache_bin_info[i].num_cached_max > 0)
            cache_bin->init(thread_cache_bin_info[i], memory, current_offset);
        else
            cache_bin->initDisabled(thread_cache_bin_info[i].num_cached_max);
    }
    /// Initialize all disabled bins to a state that can safely and efficiently fail all fastpath alloc / free, so
    /// that no additional check around tcache_nbins is needed on the fast path. Yet we still store the ncached_max in
    /// the bin_info for future usage.
    for (unsigned i = thread_cache_num_bins; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        CacheBin * cache_bin = &thread_cache->bins[i];
        cache_bin->initDisabled(thread_cache_bin_info[i].num_cached_max);
        ALLOCATOR_ASSERT(threadCacheBinDisabled(i, cache_bin, thread_cache->thread_cache_slow));
    }

    cacheBinPostincrement(memory, current_offset);
    if constexpr (config::debug)
    {
        /// Sanity check that the whole stack is used.
        size_t size;
        size_t alignment;
        cacheBinInfoComputeAlloc(thread_cache_bin_info, thread_cache_num_bins, size, alignment);
        ALLOCATOR_ASSERT(current_offset == size);
    }
}

}

/// jemalloc: tcache_ncached_max_compute
unsigned threadCacheNumCachedMaxCompute(SizeClassIdx size_class_idx)
{
    if (size_class_idx >= SIZE_CLASS_NUM_BINS)
        return options.thread_cache_num_slots_large;
    unsigned slab_num_regions = bin_infos[size_class_idx].num_regions;

    /// We may modify these values; start with the opt versions.
    unsigned num_slots_small_min = options.thread_cache_num_slots_small_min;
    unsigned num_slots_small_max = options.thread_cache_num_slots_small_max;

    /// Clamp values to meet our constraints -- even, nonzero, min < max, and suitable for a cache bin size.
    if (options.thread_cache_num_slots_small_max > CACHE_BIN_NUM_CACHED_MAX)
        num_slots_small_max = CACHE_BIN_NUM_CACHED_MAX;
    if (num_slots_small_min % 2 != 0)
        ++num_slots_small_min;
    if (num_slots_small_max % 2 != 0)
        --num_slots_small_max;
    if (num_slots_small_min < 2)
        num_slots_small_min = 2;
    if (num_slots_small_max < 2)
        num_slots_small_max = 2;
    if (num_slots_small_min > num_slots_small_max)
        num_slots_small_min = num_slots_small_max;

    unsigned candidate;
    if (options.log2_thread_cache_num_slots_multiplier < 0)
        candidate = slab_num_regions >> (-options.log2_thread_cache_num_slots_multiplier);
    else
        candidate = slab_num_regions << options.log2_thread_cache_num_slots_multiplier;
    if (candidate % 2 != 0)
    {
        /// We need the candidate size to be even -- we assume that we can divide by two and get a positive number
        /// (e.g. when flushing).
        ++candidate;
    }
    if (candidate <= num_slots_small_min)
        return num_slots_small_min;
    else if (candidate <= num_slots_small_max)
        return candidate;
    else
        return num_slots_small_max;
}

/// jemalloc: tcache_bin_info_compute
void threadCacheBinInfoCompute(CacheBinInfo * thread_cache_bin_info)
{
    /// Compute the values for each bin, but for bins with indices larger than tcache_nbins, no items will be cached.
    for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
    {
        unsigned num_cached_max = threadCacheGetDefaultNumCachedMaxSet(i) ? unsigned(options.thread_cache_num_cached_max[i])
                                                                          : threadCacheNumCachedMaxCompute(i);
        ALLOCATOR_ASSERT(num_cached_max <= CACHE_BIN_NUM_CACHED_MAX);
        thread_cache_bin_info[i].init(static_cast<CacheBinSize>(num_cached_max));
    }
}

namespace
{

/// jemalloc: tcache_stack_alloc_impl
void * threadCacheStackAllocImpl(ThreadState * thread_state, size_t size, size_t alignment)
{
    if (cacheBinStackUseTransparentHugePages())
    {
        /// Alignment is ignored since it comes from THP.
        ALLOCATOR_ASSERT(alignment == QUANTUM);
        return b0AllocThreadCacheStack(thread_state, size);
    }
    size = size_classes::alignedSizeToUsableSize(size, alignment);
    return internalAllocateAlignedFull(thread_state, size, alignment, true, nullptr, true, arenaGet(nullptr, 0, true));
}

}

constinit void * (*thread_cache_stack_alloc)(ThreadState * thread_state, size_t size, size_t alignment) = threadCacheStackAllocImpl;

namespace
{

/// jemalloc: tsd_tcache_data_init_impl
bool threadStateThreadCacheDataInitImpl(ThreadState & thread_state, Arena * arena, const CacheBinInfo * thread_cache_bin_info)
{
    ThreadCacheSlow * thread_cache_slow = thread_state.threadCacheSlowGet();
    ThreadCache * thread_cache = thread_state.threadCacheGet();

    ALLOCATOR_ASSERT(thread_cache->bins[0].stillZeroInitialized());
    unsigned thread_cache_num_bins = threadCacheNumBinsGet(thread_cache_slow);
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(thread_cache_bin_info, thread_cache_num_bins, size, alignment);

    void * memory = thread_cache_stack_alloc(&thread_state, size, alignment);
    if (memory == nullptr)
        return true;

    threadCacheInit(thread_state, thread_cache_slow, thread_cache, memory, thread_cache_bin_info);
    /// Initialization is a bit tricky here. After malloc init is done, all threads can rely on `arenaChoose` and
    /// associate the tcache accordingly. However, the thread that does the actual malloc bootstrapping relies on a
    /// functional tsd, and it can only rely on a0. In that case, we associate its tcache to a0 temporarily, and later
    /// on `arenaChooseHard` will re-associate properly.
    thread_cache_slow->arena = nullptr;
    if (!mallocInitialized())
    {
        /// If in initialization, assign to a0.
        arena = arenaGet(&thread_state, 0, false);
        threadCacheArenaAssociate(&thread_state, thread_cache_slow, thread_cache, arena);
    }
    else
    {
        if (arena == nullptr)
            arena = arenaChoose(thread_state, nullptr);
        if (ALLOCATOR_UNLIKELY(arena == nullptr))
        {
            /// Out of memory while initializing an arena. Roll back `threadCacheInit` (the same way as
            /// `threadCacheCleanup` does), so that the tcache stays uninitialized, and report the error.
            if (cacheBinStackUseTransparentHugePages())
                b0DeallocateThreadCacheStack(&thread_state, memory);
            else
                internalDeallocateFull(&thread_state, memory, nullptr, nullptr, true, true);
            memset(static_cast<void *>(thread_cache->bins), 0, sizeof(CacheBin) * THREAD_CACHE_NUM_BINS_MAX);
            return true;
        }
        /// This may happen if thread.tcache.enabled is used.
        if (thread_cache_slow->arena == nullptr)
            threadCacheArenaAssociate(&thread_state, thread_cache_slow, thread_cache, arena);
    }
    ALLOCATOR_ASSERT(arena == thread_cache_slow->arena);

    return false;
}

/// Initializes the automatic tcache (embedded in TSD). Returns true on error.
/// jemalloc: tsd_tcache_data_init
bool threadStateThreadCacheDataInit(ThreadState & thread_state, Arena * arena, const CacheBinInfo * thread_cache_bin_info)
{
    ALLOCATOR_ASSERT(thread_cache_bin_info != nullptr);
    bool error = threadStateThreadCacheDataInitImpl(thread_state, arena, thread_cache_bin_info);
    if (ALLOCATOR_UNLIKELY(error))
    {
        /// Disable the tcache before calling `writeMessage` to avoid recursive allocations through libc hooks.
        thread_state.thread_cache_enabled = false;
        thread_state.slowUpdate();
        writeMessage("<jemalloc>: Failed to allocate tcache data\n");
        if (options.abort)
            abort();
    }
    return error;
}

}

/// Creates a manual tcache for the `tcache.create` mallctl.
/// jemalloc: tcache_create_explicit
ThreadCache * threadCacheCreateExplicit(ThreadState & thread_state)
{
    /// We place the cache bin stacks, then the `ThreadCache`, then the `ThreadCacheSlow` (whose `dynamic_alloc` points
    /// to the beginning of the whole allocation, for freeing). This makes sure the cache bins have the requested
    /// alignment.
    unsigned thread_cache_num_bins = global_do_not_change_thread_cache_num_bins;
    size_t thread_cache_size;
    size_t alignment;
    cacheBinInfoComputeAlloc(threadCacheGetDefaultNumCachedMax(), thread_cache_num_bins, thread_cache_size, alignment);

    size_t size = thread_cache_size + sizeof(ThreadCache) + sizeof(ThreadCacheSlow);
    /// Naturally align the pointer stacks.
    size = alignmentCeiling(size, sizeof(void *));
    size = size_classes::alignedSizeToUsableSize(size, alignment);

    void * memory = internalAllocateAlignedFull(&thread_state, size, alignment, true, nullptr, true, arenaGet(nullptr, 0, true));
    if (memory == nullptr)
        return nullptr;
    ThreadCache * thread_cache = reinterpret_cast<ThreadCache *>(static_cast<std::byte *>(memory) + thread_cache_size);
    ThreadCacheSlow * thread_cache_slow
        = reinterpret_cast<ThreadCacheSlow *>(static_cast<std::byte *>(memory) + thread_cache_size + sizeof(ThreadCache));
    threadCacheDefaultSettingsInit(thread_cache_slow);
    threadCacheInit(thread_state, thread_cache_slow, thread_cache, memory, threadCacheGetDefaultNumCachedMax());

    Arena * arena = arenaChooseInternal(thread_state, nullptr);
    if (ALLOCATOR_UNLIKELY(arena == nullptr))
    {
        /// Out of memory while initializing an arena. The tcache is not associated or published yet, so it is enough
        /// to free its memory. jemalloc passes the null arena to `tcache_arena_associate` here.
        internalDeallocateFull(&thread_state, memory, nullptr, nullptr, true, true);
        return nullptr;
    }
    threadCacheArenaAssociate(&thread_state, thread_cache_slow, thread_cache, arena);

    return thread_cache;
}

/// Called upon tsd initialization.
/// jemalloc: tsd_tcache_enabled_data_init
bool threadCacheThreadStateDataInit(ThreadState & thread_state)
{
    thread_state.thread_cache_enabled = options.thread_cache;
    /// The tcache is not available yet, but we need to set up its tcache_nbins in advance.
    threadCacheDefaultSettingsInit(thread_state.threadCacheSlowGet());
    thread_state.slowUpdate();

    if (options.thread_cache)
    {
        /// Trigger tcache init.
        return threadStateThreadCacheDataInit(thread_state, nullptr, threadCacheGetDefaultNumCachedMax());
    }

    return false;
}

/// jemalloc: tcache_enabled_set
void threadCacheEnabledSet(ThreadState & thread_state, bool enabled)
{
    bool was_enabled = thread_state.thread_cache_enabled;

    if (!was_enabled && enabled)
    {
        if (threadStateThreadCacheDataInit(thread_state, nullptr, threadCacheGetDefaultNumCachedMax()))
            return;
    }
    else if (was_enabled && !enabled)
    {
        threadCacheCleanup(thread_state);
    }
    /// Commit the state last. The above calls check the current state.
    thread_state.thread_cache_enabled = enabled;
    thread_state.slowUpdate();
}

/// jemalloc: thread_tcache_max_set
bool threadThreadCacheMaxSet(ThreadState & thread_state, size_t thread_cache_max)
{
    ALLOCATOR_ASSERT(thread_cache_max <= THREAD_CACHE_MAX_CLASS_LIMIT);
    ALLOCATOR_ASSERT(thread_cache_max == size_classes::sizeToUsableSize(thread_cache_max));
    ThreadCache * thread_cache = thread_state.threadCacheGet();
    /// The slow part lives in the TSD (`thread_cache->thread_cache_slow` is null when the tcache was never initialized).
    ThreadCacheSlow * thread_cache_slow = thread_state.threadCacheSlowGet();
    CacheBinInfo thread_cache_bin_info[THREAD_CACHE_NUM_BINS_MAX] = {};
    bool result = false;
    ALLOCATOR_ASSERT(thread_cache != nullptr && thread_cache_slow != nullptr);

    bool enabled = threadCacheAvailable(thread_state);
    Arena * assigned_arena = nullptr;
    if (enabled)
    {
        assigned_arena = thread_cache_slow->arena;
        /// Carry over the bin settings during the reboot.
        threadCacheBinSettingsBackup(thread_cache, thread_cache_bin_info);
        /// Shutdown and reboot the tcache for a clean slate.
        threadCacheCleanup(thread_state);
    }

    /// Still set tcache_nbins of the tcache even if the tcache is not available yet because the values are stored in
    /// the TSD and are always available for changing.
    threadCacheMaxSet(thread_cache_slow, thread_cache_max);

    if (enabled)
        result = threadStateThreadCacheDataInit(thread_state, assigned_arena, thread_cache_bin_info);

    ALLOCATOR_ASSERT(threadCacheNumBinsGet(thread_cache_slow) == size_classes::sizeToIndex(thread_cache_max) + 1);
    return result;
}

/// jemalloc: tcache_bins_ncached_max_write
bool threadCacheBinsNumCachedMaxWrite(ThreadState & thread_state, const char * settings, size_t len)
{
    ALLOCATOR_ASSERT(threadCacheAvailable(thread_state));
    ALLOCATOR_ASSERT(len != 0);
    ThreadCache * thread_cache = thread_state.threadCacheGet();
    ALLOCATOR_ASSERT(thread_cache != nullptr);
    CacheBinInfo thread_cache_bin_info[THREAD_CACHE_NUM_BINS_MAX];
    threadCacheBinSettingsBackup(thread_cache, thread_cache_bin_info);

    if (threadCacheBinInfoSettingsParse(
            settings, len, [&](SizeClassIdx i, uint16_t num_cached_max) { thread_cache_bin_info[i].init(num_cached_max); }))
        return true;

    Arena * assigned_arena = thread_cache->thread_cache_slow->arena;
    threadCacheCleanup(thread_state);
    return threadStateThreadCacheDataInit(thread_state, assigned_arena, thread_cache_bin_info);
}

/// --- Flush and destruction -----------------------------------------------------------------------------------------

namespace
{

/// jemalloc: tcache_flush_cache
void threadCacheFlushCache(ThreadState & thread_state, ThreadCache * thread_cache)
{
    ThreadCacheSlow * thread_cache_slow = thread_cache->thread_cache_slow;
    ALLOCATOR_ASSERT(thread_cache_slow->arena != nullptr);

    for (unsigned i = 0; i < threadCacheNumBinsGet(thread_cache_slow); ++i)
    {
        CacheBin * cache_bin = &thread_cache->bins[i];
        if (threadCacheBinDisabled(i, cache_bin, thread_cache_slow))
            continue;
        if (i < SIZE_CLASS_NUM_BINS)
            threadCacheBinFlushSmall(thread_state, thread_cache, cache_bin, i, 0);
        else
            threadCacheBinFlushLarge(thread_state, thread_cache, cache_bin, i, 0);
        if constexpr (config::stats)
            ALLOCATOR_ASSERT(cache_bin->thread_cache_stats.num_requests == 0);
    }
}

/// jemalloc: tcache_destroy
void threadCacheDestroy(ThreadState & thread_state, ThreadCache * thread_cache, bool thread_state_thread_cache)
{
    ThreadCacheSlow * thread_cache_slow = thread_cache->thread_cache_slow;
    threadCacheFlushCache(thread_state, thread_cache);
    Arena * arena = thread_cache_slow->arena;
    threadCacheArenaDissociate(&thread_state, thread_cache_slow, thread_cache);

    if (thread_state_thread_cache)
    {
        [[maybe_unused]] CacheBin * cache_bin = &thread_cache->bins[0];
        cache_bin->assertEmpty();
    }
    if (thread_state_thread_cache && cacheBinStackUseTransparentHugePages())
        b0DeallocateThreadCacheStack(&thread_state, thread_cache_slow->dynamic_alloc);
    else
        internalDeallocateFull(&thread_state, thread_cache_slow->dynamic_alloc, nullptr, nullptr, true, true);

    /// The deallocation and tcache flush above may not trigger decay since we are on the tcache shutdown path
    /// (potentially with non-nominal tsd). Manually trigger decay to avoid pathological cases. Also include arena 0
    /// because the tcache array is allocated from it.
    arenaDecay(&thread_state, arenaGet(&thread_state, 0, false), false, false);

    if (arenaNumThreadsGet(arena, false) == 0 && !backgroundThreadEnabled())
    {
        /// Force purging when no threads are assigned to the arena anymore.
        arenaDecay(&thread_state, arena, /* is_background_thread */ false, /* all */ true);
    }
    else
    {
        arenaDecay(&thread_state, arena, /* is_background_thread */ false, /* all */ false);
    }
}

}

/// jemalloc: tcache_flush
void threadCacheFlush(ThreadState & thread_state)
{
    ALLOCATOR_ASSERT(threadCacheAvailable(thread_state));
    threadCacheFlushCache(thread_state, thread_state.threadCacheGet());
}

/// For the automatic tcache (embedded in TSD) only.
/// jemalloc: tcache_cleanup
void threadCacheCleanup(ThreadState & thread_state)
{
    ThreadCache * thread_cache = thread_state.threadCacheGet();
    if (!threadCacheAvailable(thread_state))
    {
        ALLOCATOR_ASSERT(thread_state.thread_cache_enabled == false);
        ALLOCATOR_ASSERT(thread_cache->bins[0].stillZeroInitialized());
        return;
    }
    ALLOCATOR_ASSERT(thread_state.thread_cache_enabled);
    ALLOCATOR_ASSERT(!thread_cache->bins[0].stillZeroInitialized());

    threadCacheDestroy(thread_state, thread_cache, true);
    /// Make sure all bins used are reinitialized to the clean state.
    memset(static_cast<void *>(thread_cache->bins), 0, sizeof(CacheBin) * THREAD_CACHE_NUM_BINS_MAX);
}

/// jemalloc: tcache_stats_merge
void threadCacheStatsMerge(ThreadState * thread_state, ThreadCache * thread_cache, Arena * arena)
{
    static_assert(config::stats);

    /// Merge and reset tcache stats.
    for (unsigned i = 0; i < threadCacheNumBinsGet(thread_cache->thread_cache_slow); ++i)
    {
        CacheBin * cache_bin = &thread_cache->bins[i];
        if (threadCacheBinDisabled(i, cache_bin, thread_cache->thread_cache_slow))
            continue;
        if (i < SIZE_CLASS_NUM_BINS)
        {
            Bin * bin = binChoose(thread_state, arena, i, nullptr);
            bin->lock.lock(thread_state);
            bin->stats.num_requests += cache_bin->thread_cache_stats.num_requests;
            bin->lock.unlock(thread_state);
        }
        else
        {
            arenaStatsLargeFlushNumRequestsAdd(thread_state, &arena->stats, i, cache_bin->thread_cache_stats.num_requests);
        }
        cache_bin->thread_cache_stats.num_requests = 0;
    }
}

/// --- Explicit tcaches ----------------------------------------------------------------------------------------------

namespace
{

/// Returns true on error.
/// jemalloc: tcaches_create_prep
bool explicitThreadCachesCreatePrepare(ThreadState & thread_state, Base * base)
{
    if (explicit_thread_caches == nullptr)
    {
        explicit_thread_caches
            = static_cast<ThreadCaches *>(base->alloc(&thread_state, sizeof(ThreadCache *) * (MALLOCX_THREAD_CACHE_MAX + 1), CACHE_LINE));
        if (explicit_thread_caches == nullptr)
            return true;
    }

    if (explicit_thread_caches_available == nullptr && explicit_thread_caches_past > MALLOCX_THREAD_CACHE_MAX)
        return true;

    return false;
}

/// jemalloc: tcaches_elm_remove
ThreadCache * explicitThreadCachesElementRemove(ThreadState & /*tsd*/, ThreadCaches * element, bool allow_reinit)
{
    if (element->thread_cache == nullptr)
        return nullptr;
    ThreadCache * thread_cache = element->thread_cache;
    if (allow_reinit)
        element->thread_cache = EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT;
    else
        element->thread_cache = nullptr;

    if (thread_cache == EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT)
        return nullptr;
    return thread_cache;
}

}

/// jemalloc: tcaches_create
bool explicitThreadCachesCreate(ThreadState & thread_state, Base * base, unsigned & r_idx)
{
    bool error;

    explicit_thread_caches_mutex.lock(&thread_state);

    if (explicitThreadCachesCreatePrepare(thread_state, base))
    {
        error = true;
    }
    else if (ThreadCache * thread_cache = threadCacheCreateExplicit(thread_state); thread_cache == nullptr)
    {
        error = true;
    }
    else
    {
        ThreadCaches * element;
        if (explicit_thread_caches_available != nullptr)
        {
            element = explicit_thread_caches_available;
            explicit_thread_caches_available = explicit_thread_caches_available->next;
            element->thread_cache = thread_cache;
            r_idx = static_cast<unsigned>(element - explicit_thread_caches);
        }
        else
        {
            element = &explicit_thread_caches[explicit_thread_caches_past];
            element->thread_cache = thread_cache;
            r_idx = explicit_thread_caches_past;
            ++explicit_thread_caches_past;
        }
        error = false;
    }

    explicit_thread_caches_mutex.unlock(&thread_state);
    return error;
}

/// jemalloc: tcaches_flush
void explicitThreadCachesFlush(ThreadState & thread_state, unsigned idx)
{
    explicit_thread_caches_mutex.lock(&thread_state);
    ThreadCache * thread_cache = explicitThreadCachesElementRemove(thread_state, &explicit_thread_caches[idx], true);
    explicit_thread_caches_mutex.unlock(&thread_state);
    if (thread_cache != nullptr)
    {
        /// Destroy the tcache; recreate in `explicitThreadCachesGet` if needed.
        threadCacheDestroy(thread_state, thread_cache, false);
    }
}

/// jemalloc: tcaches_destroy
void explicitThreadCachesDestroy(ThreadState & thread_state, unsigned idx)
{
    explicit_thread_caches_mutex.lock(&thread_state);
    ThreadCaches * element = &explicit_thread_caches[idx];
    ThreadCache * thread_cache = explicitThreadCachesElementRemove(thread_state, element, false);
    element->next = explicit_thread_caches_available;
    explicit_thread_caches_available = element;
    explicit_thread_caches_mutex.unlock(&thread_state);
    if (thread_cache != nullptr)
        threadCacheDestroy(thread_state, thread_cache, false);
}

/// --- Boot, fork ----------------------------------------------------------------------------------------------------

/// jemalloc: tcache_boot
bool threadCacheBoot(ThreadState * /*tsdn*/, Base * /*base*/)
{
    global_do_not_change_thread_cache_max_class = size_classes::sizeToUsableSize(options.thread_cache_max);
    ALLOCATOR_ASSERT(global_do_not_change_thread_cache_max_class <= THREAD_CACHE_MAX_CLASS_LIMIT);
    global_do_not_change_thread_cache_num_bins = size_classes::sizeToIndex(global_do_not_change_thread_cache_max_class) + 1;
    /// Pre-compute the default bin info. After this, it should not be modified and should always be accessed using
    /// `threadCacheGetDefaultNumCachedMax`.
    threadCacheBinInfoCompute(thread_cache_default_num_cached_max);

    if (explicit_thread_caches_mutex.init("tcaches", MutexRank::EXPLICIT_THREAD_CACHES, MutexLockOrder::RankExclusive))
        return true;

    return false;
}

/// jemalloc: tcache_prefork
void threadCachePrefork(ThreadState * thread_state)
{
    explicit_thread_caches_mutex.prefork(thread_state);
}

/// jemalloc: tcache_postfork_parent
void threadCachePostforkParent(ThreadState * thread_state)
{
    explicit_thread_caches_mutex.postforkParent(thread_state);
}

/// jemalloc: tcache_postfork_child
void threadCachePostforkChild(ThreadState * thread_state)
{
    explicit_thread_caches_mutex.postforkChild(thread_state);
}

/// jemalloc: tcache_assert_initialized
void threadCacheAssertInitialized([[maybe_unused]] ThreadCache * thread_cache)
{
    ALLOCATOR_ASSERT(!thread_cache->bins[0].stillZeroInitialized());
}

}
