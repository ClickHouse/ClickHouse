#include <allocator/Arena.h>

#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Format.h>
#include <allocator/Options.h>
#include <allocator/Sanitizer.h>
#include <allocator/ThreadCache.h>

#include <cstdlib>
#include <cstring>
#include <new>

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

/// The runtime defaults ("arenas.dirty_decay_ms", "arenas.muzzy_decay_ms").
/// jemalloc: dirty_decay_ms_default, muzzy_decay_ms_default (static)
static constinit std::atomic<ssize_t> dirty_decay_ms_default{0};
static constinit std::atomic<ssize_t> muzzy_decay_ms_default{0};

constinit DivisionInfo arena_bin_idx_division_info[SIZE_CLASS_NUM_BINS] = {};

constinit size_t oversize_threshold = OVERSIZE_THRESHOLD_DEFAULT;

constinit uint32_t arena_bin_offsets[SIZE_CLASS_NUM_BINS] = {};
constinit unsigned arena_num_bins_total = 0;

constinit unsigned huge_arena_idx = 0;

const ArenaConfig arena_config_default = {
    /* .extent_hooks = */ &extent_hooks_default_extent_hooks,
    /* .metadata_use_hooks = */ true,
};

/// --- Stats ---------------------------------------------------------------------------------------------------------

/// jemalloc: arena_basic_stats_merge
void arenaBasicStatsMerge(
    ThreadState * /*tsdn*/,
    Arena * arena,
    unsigned * num_threads,
    const char ** sbrk,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * num_active,
    size_t * num_dirty,
    size_t * num_muzzy)
{
    *num_threads += arenaNumThreadsGet(arena, false);
    *sbrk = sbrk_precedence_names[unsigned(arenaSbrkPrecedenceGet(arena))];
    *dirty_decay_ms = arenaDecayMsGet(arena, extent_state_dirty);
    *muzzy_decay_ms = arenaDecayMsGet(arena, extent_state_muzzy);
    arena->page_allocator_shard.basicStatsMerge(num_active, num_dirty, num_muzzy);
}

/// jemalloc: arena_stats_merge
void arenaStatsMerge(
    ThreadState * thread_state,
    Arena * arena,
    unsigned * num_threads,
    const char ** sbrk,
    ssize_t * dirty_decay_ms,
    ssize_t * muzzy_decay_ms,
    size_t * num_active,
    size_t * num_dirty,
    size_t * num_muzzy,
    ArenaStats * arena_stats,
    BinStatsData * bin_stats,
    ArenaStatsLarge * large_stats,
    PageAllocatorExtentStats * extent_stats)
{
    static_assert(config::stats);

    arenaBasicStatsMerge(thread_state, arena, num_threads, sbrk, dirty_decay_ms, muzzy_decay_ms, num_active, num_dirty, num_muzzy);

    size_t base_allocated;
    size_t base_extent_allocated;
    size_t base_radix_tree_allocated;
    size_t base_resident;
    size_t base_mapped;
    size_t metadata_transparent_huge_pages;
    arena->base->statsGet(
        thread_state,
        &base_allocated,
        &base_extent_allocated,
        &base_radix_tree_allocated,
        &base_resident,
        &base_mapped,
        &metadata_transparent_huge_pages);
    size_t page_allocator_mapped_size = arena->page_allocator_shard.page_allocator.mapped();
    arena_stats->mapped += base_mapped + page_allocator_mapped_size;
    arena_stats->resident += base_resident;

    /// LOCKEDINT_MTX_LOCK: no stats mutex.

    arena_stats->base += base_allocated;
    arena_stats->metadata_extent += base_extent_allocated;
    arena_stats->metadata_radix_tree += base_radix_tree_allocated;
    /// atomic_load_add_store_zu
    arena_stats->internal.store(arena_stats->internal.load(std::memory_order_relaxed) + arenaInternalGet(arena), std::memory_order_relaxed);
    arena_stats->metadata_transparent_huge_pages += metadata_transparent_huge_pages;

    for (SizeClassIdx i = 0; i < SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS; ++i)
    {
        /// ndalloc should be read before nmalloc, since otherwise it is possible for ndalloc to be incremented, and the
        /// following can become true: ndalloc > nmalloc.
        uint64_t num_deallocations = arena->stats.large_stats[i].num_deallocations.read();
        large_stats[i].num_deallocations.incrementUnsynchronized(num_deallocations);
        arena_stats->num_deallocations_large += num_deallocations;

        uint64_t num_allocations = arena->stats.large_stats[i].num_allocations.read();
        large_stats[i].num_allocations.incrementUnsynchronized(num_allocations);
        arena_stats->num_allocations_large += num_allocations;

        uint64_t num_requests = arena->stats.large_stats[i].num_requests.read();
        large_stats[i].num_requests.incrementUnsynchronized(num_allocations + num_requests);
        arena_stats->num_requests_large += num_allocations + num_requests;

        /// nfill == nmalloc for large currently.
        large_stats[i].num_fills.incrementUnsynchronized(num_allocations);
        arena_stats->num_fills_large += num_allocations;

        uint64_t num_flush = arena->stats.large_stats[i].num_flushes.read();
        large_stats[i].num_flushes.incrementUnsynchronized(num_flush);
        arena_stats->num_flushes_large += num_flush;

        ALLOCATOR_ASSERT(num_allocations >= num_deallocations);
        ALLOCATOR_ASSERT(num_allocations - num_deallocations <= SIZE_MAX);
        size_t current_large_extents = size_t(num_allocations - num_deallocations);
        large_stats[i].current_large_extents += current_large_extents;

        uint64_t active_bytes = arena->stats.large_stats[i].active_bytes.read();
        large_stats[i].active_bytes.incrementUnsynchronized(active_bytes);
        arena_stats->allocated_large += active_bytes;
    }

    arena->page_allocator_shard.statsMerge(thread_state, &arena_stats->page_allocator_shard_stats, extent_stats, &arena_stats->resident);

    /// LOCKEDINT_MTX_UNLOCK: no stats mutex.

    /// Currently cached bytes and sanitizer-stashed bytes in tcache.
    arena_stats->thread_cache_bytes = 0;
    arena_stats->thread_cache_stashed_bytes = 0;
    arena->thread_cache_list_mutex.lock(thread_state);
    arena->cache_bin_array_descriptor_list.forEach(
        [&](CacheBinArrayDescriptor * descriptor)
        {
            for (SizeClassIdx i = 0; i < THREAD_CACHE_NUM_BINS_MAX; ++i)
            {
                CacheBin * cache_bin = &descriptor->bins[i];
                if (cache_bin->disabled())
                    continue;

                CacheBinSize num_cached;
                CacheBinSize num_stashed;
                cache_bin->numItemsGetRemote(num_cached, num_stashed);
                arena_stats->thread_cache_bytes += num_cached * size_classes::indexToSize(i);
                arena_stats->thread_cache_stashed_bytes += num_stashed * size_classes::indexToSize(i);
            }
        });
    arena->thread_cache_list_mutex.profilingRead(thread_state, arena_stats->mutex_profiling_data[arena_profiling_mutex_thread_cache_list]);
    arena->thread_cache_list_mutex.unlock(thread_state);

    /// Gather per arena mutex profiling data.
    arena->large_mutex.lock(thread_state);
    arena->large_mutex.profilingRead(thread_state, arena_stats->mutex_profiling_data[arena_profiling_mutex_large]);
    arena->large_mutex.unlock(thread_state);
    Mutex & base_mutex = arena->base->getMutex();
    base_mutex.lock(thread_state);
    base_mutex.profilingRead(thread_state, arena_stats->mutex_profiling_data[arena_profiling_mutex_base]);
    base_mutex.unlock(thread_state);
    arena->page_allocator_shard.mutexStatsRead(thread_state, arena_stats->mutex_profiling_data);

    arena_stats->uptime.copy(arena->create_time);
    arena_stats->uptime.update();
    arena_stats->uptime.subtract(arena->create_time);

    for (SizeClassIdx i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        for (unsigned j = 0; j < bin_infos[i].num_shards; ++j)
            arenaGetBin(arena, i, j)->statsMerge(thread_state, bin_stats[i]);
    }
}

/// --- Decay wiring --------------------------------------------------------------------------------------------------

static void arenaMaybeDoDeferredWork(ThreadState * thread_state, Arena * arena, Decay * decay, size_t num_pages_new);
static bool arenaDecayDirty(ThreadState * thread_state, Arena * arena, bool is_background_thread, bool all);

/// jemalloc: arena_background_thread_inactivity_check
static void arenaBackgroundThreadInactivityCheck(ThreadState * thread_state, Arena * arena, bool is_background_thread)
{
    if (!backgroundThreadEnabled() || is_background_thread)
        return;
    BackgroundThreadInfo * info = arenaBackgroundThreadInfoGet(arena);
    if (backgroundThreadIndefiniteSleep(info))
        arenaMaybeDoDeferredWork(thread_state, arena, &arena->page_allocator_shard.page_allocator.decay_dirty, 0);
}

/// jemalloc: arena_handle_deferred_work
void arenaHandleDeferredWork(ThreadState * thread_state, Arena * arena)
{
    if (arena->page_allocator_shard.page_allocator.decay_dirty.immediately())
        arenaDecayDirty(thread_state, arena, false, true);
    arenaBackgroundThreadInactivityCheck(thread_state, arena, false);
}

/// In situations where we're not forcing a decay (i.e. because the user specifically requested it), should we purge
/// ourselves, or wait for the background thread to get to it.
/// jemalloc: arena_decide_unforced_purge_eagerness
static PageAllocatorPurgeEagerness arenaDecideUnforcedPurgeEagerness(bool is_background_thread)
{
    if (is_background_thread)
        return PAGE_ALLOCATOR_PURGE_ALWAYS;
    else if (!is_background_thread && backgroundThreadEnabled())
        return PAGE_ALLOCATOR_PURGE_NEVER;
    else
        return PAGE_ALLOCATOR_PURGE_ON_EPOCH_ADVANCE;
}

/// jemalloc: arena_decay_ms_set
bool arenaDecayMsSet(ThreadState * thread_state, Arena * arena, ExtentState state, ssize_t decay_ms)
{
    PageAllocatorPurgeEagerness eagerness = arenaDecideUnforcedPurgeEagerness(/* is_background_thread */ false);
    return arena->page_allocator_shard.decayMsSet(thread_state, state, decay_ms, eagerness);
}

/// jemalloc: arena_decay_ms_get
ssize_t arenaDecayMsGet(Arena * arena, ExtentState state)
{
    return arena->page_allocator_shard.decayMsGet(state);
}

/// Returns true if another thread is decaying (the decay mutex is busy).
/// jemalloc: arena_decay_impl
static bool arenaDecayImpl(
    ThreadState * thread_state,
    Arena * arena,
    Decay * decay,
    DecayStats * decay_stats,
    ExtentCache * extent_cache,
    bool is_background_thread,
    bool all)
{
    if (all)
    {
        decay->mutex.lock(thread_state);
        arena->page_allocator_shard.page_allocator.decayAll(thread_state, decay, decay_stats, extent_cache, /* fully_decay */ all);
        decay->mutex.unlock(thread_state);
        return false;
    }

    if (!decay->mutex.tryLock(thread_state))
    {
        /// No need to wait if another thread is in progress.
        return true;
    }
    PageAllocatorPurgeEagerness eagerness = arenaDecideUnforcedPurgeEagerness(is_background_thread);
    bool epoch_advanced
        = arena->page_allocator_shard.page_allocator.maybeDecayPurge(thread_state, decay, decay_stats, extent_cache, eagerness);
    size_t num_pages_new = 0;
    if (epoch_advanced)
    {
        /// Backlog is updated on epoch advance.
        num_pages_new = decay->epochNumPagesDelta();
    }
    decay->mutex.unlock(thread_state);

    if (config::background_thread && backgroundThreadEnabled() && epoch_advanced && !is_background_thread)
        arenaMaybeDoDeferredWork(thread_state, arena, decay, num_pages_new);

    return false;
}

/// jemalloc: arena_decay_dirty
static bool arenaDecayDirty(ThreadState * thread_state, Arena * arena, bool is_background_thread, bool all)
{
    PageAllocator & page_allocator = arena->page_allocator_shard.page_allocator;
    return arenaDecayImpl(
        thread_state,
        arena,
        &page_allocator.decay_dirty,
        &page_allocator.stats->decay_dirty,
        &page_allocator.extent_cache_dirty,
        is_background_thread,
        all);
}

/// jemalloc: arena_decay_muzzy
static bool arenaDecayMuzzy(ThreadState * thread_state, Arena * arena, bool is_background_thread, bool all)
{
    if (arena->page_allocator_shard.dontDecayMuzzy())
        return false;
    PageAllocator & page_allocator = arena->page_allocator_shard.page_allocator;
    return arenaDecayImpl(
        thread_state,
        arena,
        &page_allocator.decay_muzzy,
        &page_allocator.stats->decay_muzzy,
        &page_allocator.extent_cache_muzzy,
        is_background_thread,
        all);
}

/// jemalloc: arena_decay
void arenaDecay(ThreadState * thread_state, Arena * arena, bool is_background_thread, bool all)
{
    if (all)
    {
        /// We should take a purge of "all" to mean "save as much memory as possible", including flushing any caches
        /// (for situations like thread death, or manual purge calls).
        arena->page_allocator_shard.flush(thread_state);
    }
    if (arenaDecayDirty(thread_state, arena, is_background_thread, all))
        return;
    arenaDecayMuzzy(thread_state, arena, is_background_thread, all);
}

/// jemalloc: arena_should_decay_early
static bool arenaShouldDecayEarly(
    ThreadState * thread_state,
    Arena * /*arena*/,
    Decay * decay,
    BackgroundThreadInfo * info,
    Nanoseconds * remaining_sleep,
    size_t num_pages_new)
{
    backgroundThreadInfoMutex(info).assertOwner(thread_state);

    if (!decay->mutex.tryLock(thread_state))
        return false;

    if (!decay->gradually())
    {
        decay->mutex.unlock(thread_state);
        return false;
    }

    remaining_sleep->init(backgroundThreadWakeupTimeGet(info));
    if (remaining_sleep->compare(decay->epoch) <= 0)
    {
        decay->mutex.unlock(thread_state);
        return false;
    }
    remaining_sleep->subtract(decay->epoch);
    if (num_pages_new > 0)
    {
        uint64_t num_purge_new = decay->numPagesPurgeIn(*remaining_sleep, num_pages_new);
        backgroundThreadNumPagesToPurgeNew(info) += num_purge_new;
    }
    decay->mutex.unlock(thread_state);
    return backgroundThreadNumPagesToPurgeNew(info) > ARENA_DEFERRED_PURGE_NUM_PAGES_THRESHOLD;
}

/// Check if deferred work needs to be done sooner than planned. For decay we might want to wake up earlier because of
/// an influx of dirty pages. Rather than waiting for previously estimated time, we proactively purge those pages. If
/// background thread sleeps indefinitely, always wake up because some deferred work has been generated.
/// jemalloc: arena_maybe_do_deferred_work
static void arenaMaybeDoDeferredWork(ThreadState * thread_state, Arena * arena, Decay * decay, size_t num_pages_new)
{
    BackgroundThreadInfo * info = arenaBackgroundThreadInfoGet(arena);
    Mutex & info_mutex = backgroundThreadInfoMutex(info);
    if (!info_mutex.tryLock(thread_state))
    {
        /// Background thread may hold the mutex for a long period of time. We'd like to avoid the variance on
        /// application threads. So keep this non-blocking, and leave the work to a future epoch.
        return;
    }
    if (backgroundThreadIsStarted(info))
    {
        Nanoseconds remaining_sleep = Nanoseconds::zero();
        if (backgroundThreadIndefiniteSleep(info))
        {
            backgroundThreadWakeupEarly(info, nullptr);
        }
        else if (arenaShouldDecayEarly(thread_state, arena, decay, info, &remaining_sleep, num_pages_new))
        {
            backgroundThreadNumPagesToPurgeNew(info) = 0;
            backgroundThreadWakeupEarly(info, &remaining_sleep);
        }
    }
    info_mutex.unlock(thread_state);
}

/// jemalloc: arena_do_deferred_work
void arenaDoDeferredWork(ThreadState * thread_state, Arena * arena)
{
    arenaDecay(thread_state, arena, true, false);
    arena->page_allocator_shard.doDeferredWork(thread_state);
}

/// --- Large extent helpers ------------------------------------------------------------------------------------------

/// jemalloc: arena_large_malloc_stats_update
static void arenaLargeMallocStatsUpdate(ThreadState * thread_state, Arena * arena, size_t usable_size)
{
    static_assert(config::stats);

    SizeClassIdx index = size_classes::sizeToIndex(usable_size);
    /// This only occurs when we have a sampled small allocation.
    if (usable_size < SIZE_CLASS_LARGE_MIN_CLASS)
    {
        ALLOCATOR_ASSERT(index < SIZE_CLASS_NUM_BINS);
        ALLOCATOR_ASSERT(usable_size >= PAGE && usable_size % PAGE == 0);
        Bin * bin = arenaGetBin(arena, index, /* binshard */ 0);
        bin->lock.lock(thread_state);
        ++bin->stats.num_allocations;
        bin->lock.unlock(thread_state);
    }
    else
    {
        ALLOCATOR_ASSERT(index >= SIZE_CLASS_NUM_BINS);
        SizeClassIdx hash_index = index - SIZE_CLASS_NUM_BINS;
        arena->stats.large_stats[hash_index].num_allocations.increment(1);
        arena->stats.large_stats[hash_index].active_bytes.increment(usable_size);
    }
}

/// jemalloc: arena_large_dalloc_stats_update
static void arenaLargeDeallocateStatsUpdate(ThreadState * thread_state, Arena * arena, size_t usable_size)
{
    static_assert(config::stats);

    SizeClassIdx index = size_classes::sizeToIndex(usable_size);
    /// This only occurs when we have a sampled small allocation.
    if (usable_size < SIZE_CLASS_LARGE_MIN_CLASS)
    {
        ALLOCATOR_ASSERT(index < SIZE_CLASS_NUM_BINS);
        ALLOCATOR_ASSERT(usable_size >= PAGE && usable_size % PAGE == 0);
        Bin * bin = arenaGetBin(arena, index, /* binshard */ 0);
        bin->lock.lock(thread_state);
        ++bin->stats.num_deallocations;
        bin->lock.unlock(thread_state);
    }
    else
    {
        ALLOCATOR_ASSERT(index >= SIZE_CLASS_NUM_BINS);
        SizeClassIdx hash_index = index - SIZE_CLASS_NUM_BINS;
        arena->stats.large_stats[hash_index].num_deallocations.increment(1);
        arena->stats.large_stats[hash_index].active_bytes.decrement(usable_size);
    }
}

/// jemalloc: arena_large_ralloc_stats_update
static void arenaLargeReallocateStatsUpdate(ThreadState * thread_state, Arena * arena, size_t old_usable_size, size_t usable_size)
{
    arenaLargeMallocStatsUpdate(thread_state, arena, usable_size);
    arenaLargeDeallocateStatsUpdate(thread_state, arena, old_usable_size);
}

/// jemalloc: arena_extent_alloc_large
Extent * arenaExtentAllocLarge(ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero)
{
    bool deferred_work_generated = false;
    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);
    size_t extent_size = usable_size + large_pad;

    bool guarded = sanitizerLargeExtentDecideGuard(thread_state, arenaGetExtentHooks(arena), extent_size, alignment);

    /// - if usize >= opt.calloc_madvise_threshold,
    ///     - pa_alloc(..., zero_override = zero, ...)
    /// - otherwise,
    ///     - pa_alloc(..., zero_override = false, ...)
    ///     - use memset() to zero out memory if zero == true.
    bool zero_override = zero && (usable_size >= options.calloc_madvise_threshold);
    Extent * extent = arena->page_allocator_shard.alloc(
        thread_state, extent_size, alignment, /* slab */ false, size_class_idx, zero_override, guarded, &deferred_work_generated);

    if (extent == nullptr)
        return nullptr;

    if constexpr (config::stats)
        arenaLargeMallocStatsUpdate(thread_state, arena, usable_size);
    if (large_pad != 0)
        arenaCacheObliviousRandomize(thread_state, arena, extent, alignment);
    /// This branch should be put after the randomization so that the addr returned by `addr()` has already be
    /// randomized, if cache_oblivious is enabled.
    if (zero && !zero_override && !extent->zeroed())
    {
        void * addr = extent->addr();
        size_t extent_usable_size = extent->usableSize();
        memset(addr, 0, extent_usable_size);
    }

    return extent;
}

/// jemalloc: arena_extent_dalloc_large_prep
void arenaExtentDeallocateLargePrepare(ThreadState * thread_state, Arena * arena, Extent * extent)
{
    if constexpr (config::stats)
        arenaLargeDeallocateStatsUpdate(thread_state, arena, extent->usableSize());
}

/// jemalloc: arena_extent_ralloc_large_shrink
void arenaExtentReallocateLargeShrink(ThreadState * thread_state, Arena * arena, Extent * extent, size_t old_usable_size)
{
    size_t usable_size = extent->usableSize();

    if constexpr (config::stats)
        arenaLargeReallocateStatsUpdate(thread_state, arena, old_usable_size, usable_size);
}

/// jemalloc: arena_extent_ralloc_large_expand
void arenaExtentReallocateLargeExpand(ThreadState * thread_state, Arena * arena, Extent * extent, size_t old_usable_size)
{
    size_t usable_size = extent->usableSize();

    if constexpr (config::stats)
        arenaLargeReallocateStatsUpdate(thread_state, arena, old_usable_size, usable_size);
}

/// --- Slabs ---------------------------------------------------------------------------------------------------------

/// jemalloc: arena_slab_dalloc
void arenaSlabDeallocate(ThreadState * thread_state, Arena * arena, Extent * slab)
{
    bool deferred_work_generated = false;
    arena->page_allocator_shard.deallocate(thread_state, slab, &deferred_work_generated);
    if (deferred_work_generated)
        arenaHandleDeferredWork(thread_state, arena);
}

/// jemalloc: arena_slab_alloc
static Extent *
arenaSlabAlloc(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, unsigned bin_shard, const BinInfo & bin_info)
{
    bool deferred_work_generated = false;

    bool guarded = sanitizerSlabExtentDecideGuard(thread_state, arenaGetExtentHooks(arena));
    Extent * slab = arena->page_allocator_shard.alloc(
        thread_state,
        bin_info.slab_size,
        /* alignment */ PAGE,
        /* slab */ true,
        /* szind */ bin_idx,
        /* zero */ false,
        guarded,
        &deferred_work_generated);

    if (deferred_work_generated)
        arenaHandleDeferredWork(thread_state, arena);

    if (slab == nullptr)
        return nullptr;
    ALLOCATOR_ASSERT(slab->slab());

    /// Initialize slab internals.
    SlabData * slab_data = slab->slabData();
    slab->setNumFreeBinShard(bin_info.num_regions, bin_shard);
    bitmapInit(slab_data->bitmap, bin_info.bitmap_info, false);

    return slab;
}

/// jemalloc: arena_bin_reset
static void arenaBinReset(ThreadState & thread_state, Arena * arena, Bin * bin)
{
    ThreadState * thread_state_ptr = &thread_state;
    Extent * slab;

    bin->lock.lock(thread_state_ptr);

    if (bin->current_slab != nullptr)
    {
        slab = bin->current_slab;
        bin->current_slab = nullptr;
        bin->lock.unlock(thread_state_ptr);
        arenaSlabDeallocate(thread_state_ptr, arena, slab);
        bin->lock.lock(thread_state_ptr);
    }
    while ((slab = bin->slabs_non_full.removeFirst()) != nullptr)
    {
        bin->lock.unlock(thread_state_ptr);
        arenaSlabDeallocate(thread_state_ptr, arena, slab);
        bin->lock.lock(thread_state_ptr);
    }
    for (slab = bin->slabs_full.first(); slab != nullptr; slab = bin->slabs_full.first())
    {
        bin->slabsFullRemove(false, slab);
        bin->lock.unlock(thread_state_ptr);
        arenaSlabDeallocate(thread_state_ptr, arena, slab);
        bin->lock.lock(thread_state_ptr);
    }
    if constexpr (config::stats)
    {
        bin->stats.current_regions = 0;
        bin->stats.current_slabs = 0;
    }
    bin->lock.unlock(thread_state_ptr);
}

/// --- Profiling -----------------------------------------------------------------------------------------------------

/// jemalloc: arena_prof_promote
void arenaProfilingPromote(
    ThreadState * thread_state, void * ptr, [[maybe_unused]] size_t usable_size, [[maybe_unused]] size_t bumped_usable_size)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(arenaAllocationSize(thread_state, ptr) == bumped_usable_size);
    ALLOCATOR_ASSERT(size_classes::canUseSlab(usable_size));

    /// `config_opt_safety_checks` (redzones) is off.

    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);

    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);
    extent->setSizeClassIdx(size_class_idx);
    arena_extent_map_global.remap(thread_state, extent, size_class_idx, /* slab */ false);

    ALLOCATOR_ASSERT(arenaAllocationSize(thread_state, ptr) == usable_size);
}

/// jemalloc: arena_prof_demote
static size_t arenaProfilingDemote(ThreadState * thread_state, Extent * extent, const void * ptr)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(ptr != nullptr);
    size_t usable_size = arenaAllocationSize(thread_state, ptr);
    size_t bumped_usable_size = size_classes::alignedSizeToUsableSize(usable_size, PROFILING_SAMPLE_ALIGNMENT);
    ALLOCATOR_ASSERT(bumped_usable_size <= SIZE_CLASS_LARGE_MIN_CLASS && pageCeiling(bumped_usable_size) == bumped_usable_size);
    ALLOCATOR_ASSERT(extent->size() - bumped_usable_size <= large_pad);
    SizeClassIdx size_class_idx = size_classes::sizeToIndex(bumped_usable_size);

    extent->setSizeClassIdx(size_class_idx);
    arena_extent_map_global.remap(thread_state, extent, size_class_idx, /* slab */ false);

    ALLOCATOR_ASSERT(arenaAllocationSize(thread_state, ptr) == bumped_usable_size);

    return bumped_usable_size;
}

/// jemalloc: arena_dalloc_promoted_impl
static void arenaDeallocatePromotedImpl(ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, bool slow_path, Extent * extent)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(options.profiling);

    [[maybe_unused]] size_t usable_size = extent->usableSize();
    size_t bumped_usable_size = arenaProfilingDemote(thread_state, extent, ptr);
    /// `config_opt_safety_checks` (redzone verification) is off.
    SizeClassIdx bumped_idx = size_classes::sizeToIndex(bumped_usable_size);
    if (bumped_usable_size >= SIZE_CLASS_LARGE_MIN_CLASS && thread_cache != nullptr && bumped_idx < THREAD_CACHE_NUM_BINS_MAX
        && !threadCacheBinDisabled(bumped_idx, &thread_cache->bins[bumped_idx], thread_cache->thread_cache_slow))
    {
        threadCacheDeallocateLarge(*thread_state, thread_cache, ptr, bumped_idx, slow_path);
    }
    else
    {
        largeDeallocate(thread_state, extent);
    }
}

/// jemalloc: arena_dalloc_promoted
void arenaDeallocatePromoted(ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, bool slow_path)
{
    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    arenaDeallocatePromotedImpl(thread_state, ptr, thread_cache, slow_path, extent);
}

/// --- Reset / destroy -----------------------------------------------------------------------------------------------

/// jemalloc: arena_reset
void arenaReset(ThreadState & thread_state, Arena * arena)
{
    /// Locking in this function is unintuitive. The caller guarantees that no concurrent operations are happening in
    /// this arena, but there are still reasons that some locking is necessary:
    /// - Some of the functions in the transitive closure of calls assume appropriate locks are held, and in some cases
    ///   these locks are temporarily dropped to avoid lock order reversal or deadlock due to reentry.
    /// - mallctl("epoch", ...) may concurrently refresh stats. While strictly speaking this is a "concurrent
    ///   operation", disallowing stats refreshes would impose an inconvenient burden.
    ThreadState * thread_state_ptr = &thread_state;

    /// Large allocations.
    arena->large_mutex.lock(thread_state_ptr);

    for (Extent * extent = arena->large.first(); extent != nullptr; extent = arena->large.first())
    {
        void * ptr = extent->base();
        size_t usable_size = 0;

        arena->large_mutex.unlock(thread_state_ptr);
        AllocContext alloc_context;
        arena_extent_map_global.allocContextLookup(thread_state_ptr, ptr, &alloc_context);
        ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);

        if (config::stats || (config::profiling && options.profiling))
        {
            usable_size = alloc_context.usableSizeGet();
            ALLOCATOR_ASSERT(usable_size == arenaAllocationSize(thread_state_ptr, ptr));
        }
        /// Remove large allocation from prof sample set.
        if (config::profiling && options.profiling)
        {
            /// jemalloc: prof_free
            ProfilingInfo profiling_info;
            arenaProfilingInfoGet(thread_state, ptr, &alloc_context, &profiling_info, /* reset_recent */ true);
            if (ALLOCATOR_UNLIKELY(profilingThreadContextIsValid(profiling_info.alloc_thread_context)))
                profilingFreeSampledObject(thread_state, ptr, usable_size, &profiling_info);
        }
        if (config::profiling && options.profiling && alloc_context.size_class_idx < SIZE_CLASS_NUM_BINS)
            arenaDeallocatePromotedImpl(thread_state_ptr, ptr, /* tcache */ nullptr, /* slow_path */ true, extent);
        else
            largeDeallocate(thread_state_ptr, extent);
        arena->large_mutex.lock(thread_state_ptr);
    }
    arena->large_mutex.unlock(thread_state_ptr);

    /// Bins.
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        for (unsigned j = 0; j < bin_infos[i].num_shards; ++j)
            arenaBinReset(thread_state, arena, arenaGetBin(arena, i, j));
    }
    arena->page_allocator_shard.reset(thread_state_ptr);
}

/// jemalloc: arena_prepare_base_deletion_sync_finish
static void arenaPrepareBaseDeletionSyncFinish(ThreadState & thread_state, Mutex ** mutexes, unsigned num_mutex)
{
    for (unsigned i = 0; i < num_mutex; ++i)
    {
        mutexes[i]->lock(&thread_state);
        mutexes[i]->unlock(&thread_state);
    }
}

/// jemalloc: ARENA_DESTROY_MAX_DELAYED_MTX
static constexpr unsigned ARENA_DESTROY_MAX_DELAYED_MUTEX = 32;

/// jemalloc: arena_prepare_base_deletion_sync
static void arenaPrepareBaseDeletionSync(ThreadState & thread_state, Mutex * mutex, Mutex ** delayed_mutex, unsigned * num_delayed)
{
    if (mutex->tryLock(&thread_state))
    {
        /// No contention.
        mutex->unlock(&thread_state);
        return;
    }
    unsigned n = *num_delayed;
    ALLOCATOR_ASSERT(n < ARENA_DESTROY_MAX_DELAYED_MUTEX);
    /// Add another to the batch.
    delayed_mutex[n++] = mutex;

    if (n == ARENA_DESTROY_MAX_DELAYED_MUTEX)
    {
        arenaPrepareBaseDeletionSyncFinish(thread_state, delayed_mutex, n);
        n = 0;
    }
    *num_delayed = n;
}

/// In order to coalesce, `tryAcquireExtentNeighbor` will attempt to check neighbor extent's state to determine
/// eligibility. This means under certain conditions, the metadata from an arena can be accessed without holding any
/// locks from that arena. In order to guarantee safe memory access, the metadata and the underlying base allocator
/// needs to be kept alive, until all pending accesses are done.
///
/// 1) with `opt.retain`, the arena boundary implies the is_head state (tracked in the rtree leaf), and the coalesce
/// flow will stop at the head state branch. Therefore no cross arena metadata access possible.
///
/// 2) without `opt.retain`, the arena id needs to be read from the extent, meaning read only cross-arena metadata
/// access is possible. The coalesce attempt will stop at the arena_id mismatch, and is always under one of the ecache
/// locks. To allow safe passthrough of such metadata accesses, the loop below will iterate through all manual arenas'
/// ecache locks. As all the metadata from this base allocator have been unlinked from the rtree, after going through
/// all the relevant ecache locks, it's safe to say that a) pending accesses are all finished, and b) no new access will
/// be generated.
/// jemalloc: arena_prepare_base_deletion
static void arenaPrepareBaseDeletion(ThreadState & thread_state, Base * base_to_destroy)
{
    if (options.retain)
        return;
    unsigned destroy_idx = base_to_destroy->idxGet();
    ALLOCATOR_ASSERT(destroy_idx >= manual_arena_base);

    ThreadState * thread_state_ptr = &thread_state;
    Mutex * delayed_mutex[ARENA_DESTROY_MAX_DELAYED_MUTEX];
    unsigned num_delayed = 0;
    unsigned total = numArenasTotalGet();
    for (unsigned i = 0; i < total; ++i)
    {
        if (i == destroy_idx)
            continue;
        Arena * arena = arenaGet(thread_state_ptr, i, false);
        if (arena == nullptr)
            continue;
        PageAllocator & page_allocator = arena->page_allocator_shard.page_allocator;
        arenaPrepareBaseDeletionSync(thread_state, &page_allocator.extent_cache_dirty.mutex, delayed_mutex, &num_delayed);
        arenaPrepareBaseDeletionSync(thread_state, &page_allocator.extent_cache_muzzy.mutex, delayed_mutex, &num_delayed);
        arenaPrepareBaseDeletionSync(thread_state, &page_allocator.extent_cache_retained.mutex, delayed_mutex, &num_delayed);
    }
    arenaPrepareBaseDeletionSyncFinish(thread_state, delayed_mutex, num_delayed);
}

/// jemalloc: arena_destroy
void arenaDestroy(ThreadState & thread_state, Arena * arena)
{
    ALLOCATOR_ASSERT(arena->base->idxGet() >= num_arenas_auto);
    ALLOCATOR_ASSERT(arenaNumThreadsGet(arena, false) == 0);
    ALLOCATOR_ASSERT(arenaNumThreadsGet(arena, true) == 0);

    /// No allocations have occurred since `arenaReset` was called. Furthermore, the caller (`arena.<i>.destroy`)
    /// purged all cached extents, so only retained extents may remain and it's safe to destroy them.
    arena->page_allocator_shard.destroy(&thread_state);

    /// Remove the arena pointer from the arenas array. We rely on the fact that there is no way for the application
    /// to get a dirty read from the arenas array unless there is an inherent race in the application involving access
    /// of an arena being concurrently destroyed. The application must synchronize knowledge of the arena's validity,
    /// so as long as we use an atomic write to update the arenas array, the application will get a clean read any
    /// time after it synchronizes knowledge that the arena is no longer valid.
    arenaSet(arena->base->idxGet(), nullptr);

    /// Destroy the base allocator, which manages all metadata ever mapped by this arena. The prepare function will
    /// make sure no pending access to the metadata in this base anymore.
    Base * base = arena->base;
    arenaPrepareBaseDeletion(thread_state, base);
    base->destroy(&thread_state);
}

/// --- Small allocation ----------------------------------------------------------------------------------------------

/// jemalloc: arena_ptr_array_fill_small
CacheBinSize arenaPtrArrayFillSmall(
    ThreadState * thread_state,
    Arena * arena,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    const CacheBinSize num_fill_min,
    const CacheBinSize num_fill_max,
    CacheBinStats merge_stats)
{
    ALLOCATOR_ASSERT(num_fill_min > 0 && num_fill_min <= num_fill_max);

    const BinInfo & bin_info = bin_infos[bin_idx];
    /// Bin-local resources are used first: 1) bin->slabcur, and 2) nonfull slabs. After both are exhausted, new slabs
    /// will be allocated through `arenaSlabAlloc`.
    ///
    /// Bin lock is only taken / released right before / after the while(...) refill loop, with new slab allocation
    /// (which has its own locking) kept outside of the loop. This setup facilitates flat combining, at the cost of the
    /// nested loop (through the refill label).
    ///
    /// To optimize for cases with contention and limited resources (e.g. hugepage-backed or non-overcommit arenas),
    /// each fill-iteration gets one chance of slab_alloc, and a retry of bin local resources after the slab
    /// allocation (regardless if slab_alloc failed, because the bin lock is dropped during the slab allocation).
    ///
    /// In other words, new slab allocation is allowed, as long as there was progress since the previous slab_alloc.
    /// This is tracked with made_progress below, initialized to true to jump start the first iteration.
    ///
    /// In other words (again), the loop will only terminate early (i.e. stop with filled < nfill) after going through
    /// the three steps: a) bin local exhausted, b) unlock and slab_alloc returns null, c) re-lock and bin local fails
    /// again.
    bool made_progress = true;
    Extent * fresh_slab = nullptr;
    bool alloc_and_retry = false;
    bool is_auto = arenaIsAuto(arena);
    CacheBinSize filled = 0;
    unsigned bin_shard;
    Bin * bin = binChoose(thread_state, arena, bin_idx, &bin_shard);

    while (true) /// label_refill
    {
        bin->lock.lock(thread_state);

        while (filled < num_fill_min)
        {
            /// Try batch-fill from slabcur first.
            Extent * current_slab = bin->current_slab;
            if (current_slab != nullptr && current_slab->numFree() > 0)
            {
                /// Use up the free slots if the total filled <= nfill_max. Otherwise, fallback to nfill_min for a more
                /// conservative memory usage.
                unsigned count = current_slab->numFree();
                if (count + filled > num_fill_max)
                    count = num_fill_min - filled;

                Bin::slabRegionAllocBatch(current_slab, bin_info, count, &array->ptr[filled]);
                made_progress = true;
                filled = CacheBinSize(filled + count);
                continue;
            }
            /// Next try refilling slabcur from nonfull slabs.
            if (!bin->refillCurrentSlabNoFreshSlab(thread_state, is_auto))
            {
                ALLOCATOR_ASSERT(bin->current_slab != nullptr);
                continue;
            }

            /// Then see if a new slab was reserved already.
            if (fresh_slab != nullptr)
            {
                bin->refillCurrentSlabWithFreshSlab(thread_state, bin_idx, fresh_slab);
                ALLOCATOR_ASSERT(bin->current_slab != nullptr);
                fresh_slab = nullptr;
                continue;
            }

            /// Try slab_alloc if made progress (or never did slab_alloc).
            if (made_progress)
            {
                ALLOCATOR_ASSERT(bin->current_slab == nullptr);
                ALLOCATOR_ASSERT(fresh_slab == nullptr);
                alloc_and_retry = true;
                /// Alloc a new slab then come back.
                break;
            }

            /// OOM.
            ALLOCATOR_ASSERT(fresh_slab == nullptr);
            ALLOCATOR_ASSERT(!alloc_and_retry);
            break;
        }

        if (config::stats && !alloc_and_retry)
        {
            bin->stats.num_allocations += filled;
            bin->stats.num_requests += merge_stats.num_requests;
            bin->stats.current_regions += filled;
            ++bin->stats.num_fills;
        }

        bin->lock.unlock(thread_state);

        if (alloc_and_retry)
        {
            ALLOCATOR_ASSERT(fresh_slab == nullptr);
            ALLOCATOR_ASSERT(filled < num_fill_min);
            ALLOCATOR_ASSERT(made_progress);

            fresh_slab = arenaSlabAlloc(thread_state, arena, bin_idx, bin_shard, bin_info);
            /// fresh_slab null case handled in the loop.

            alloc_and_retry = false;
            made_progress = false;
            continue;
        }
        break;
    }
    ALLOCATOR_ASSERT((filled >= num_fill_min && filled <= num_fill_max) || (fresh_slab == nullptr && !made_progress));

    /// Release if allocated but not used.
    if (fresh_slab != nullptr)
    {
        ALLOCATOR_ASSERT(fresh_slab->numFree() == bin_info.num_regions);
        arenaSlabDeallocate(thread_state, arena, fresh_slab);
        fresh_slab = nullptr;
    }

    arenaDecayTick(thread_state, arena);
    return filled;
}

/// jemalloc: arena_fill_small_fresh
size_t arenaFillSmallFresh(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, void ** ptrs, size_t num_fill, bool zero)
{
    ALLOCATOR_ASSERT(bin_idx < SIZE_CLASS_NUM_BINS);
    const BinInfo & bin_info = bin_infos[bin_idx];
    const size_t num_regions = bin_info.num_regions;
    ALLOCATOR_ASSERT(num_regions > 0);
    const size_t usable_size = bin_info.region_size;

    const bool manual_arena = !arenaIsAuto(arena);
    unsigned bin_shard;
    Bin * bin = binChoose(thread_state, arena, bin_idx, &bin_shard);

    size_t num_slab = 0;
    size_t filled = 0;
    Extent * slab = nullptr;
    ExtentListActive full_slabs;
    full_slabs.init();

    while (filled < num_fill && (slab = arenaSlabAlloc(thread_state, arena, bin_idx, bin_shard, bin_info)) != nullptr)
    {
        ALLOCATOR_ASSERT(size_t(slab->numFree()) == num_regions);
        ++num_slab;
        size_t batch = num_fill - filled;
        if (batch > num_regions)
            batch = num_regions;
        ALLOCATOR_ASSERT(batch > 0);
        Bin::slabRegionAllocBatch(slab, bin_info, unsigned(batch), &ptrs[filled]);
        ALLOCATOR_ASSERT(slab->addr() == ptrs[filled]);
        if (zero)
            memset(ptrs[filled], 0, batch * usable_size);
        filled += batch;
        if (batch == num_regions)
        {
            if (manual_arena)
                full_slabs.append(slab);
            slab = nullptr;
        }
    }

    bin->lock.lock(thread_state);
    /// Only the last slab can be non-empty, and the last slab is non-empty iff slab != null.
    if (slab != nullptr)
        bin->lowerSlab(thread_state, !manual_arena, slab);
    if (manual_arena)
        bin->slabs_full.concat(full_slabs);
    ALLOCATOR_ASSERT(full_slabs.empty());
    if constexpr (config::stats)
    {
        bin->stats.num_slabs += num_slab;
        bin->stats.current_slabs += num_slab;
        bin->stats.num_allocations += filled;
        bin->stats.num_requests += filled;
        bin->stats.current_regions += filled;
    }
    bin->lock.unlock(thread_state);

    arenaDecayTick(thread_state, arena);
    return filled;
}

/// jemalloc: arena_malloc_small
static void * arenaMallocSmall(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, bool zero)
{
    ALLOCATOR_ASSERT(bin_idx < SIZE_CLASS_NUM_BINS);
    const BinInfo & bin_info = bin_infos[bin_idx];
    size_t usable_size = size_classes::indexToSize(bin_idx);
    bool is_auto = arenaIsAuto(arena);
    unsigned bin_shard;
    Bin * bin = binChoose(thread_state, arena, bin_idx, &bin_shard);

    bin->lock.lock(thread_state);
    Extent * fresh_slab = nullptr;
    void * result = bin->mallocNoFreshSlab(thread_state, is_auto, bin_idx);
    if (result == nullptr)
    {
        bin->lock.unlock(thread_state);
        fresh_slab = arenaSlabAlloc(thread_state, arena, bin_idx, bin_shard, bin_info);
        bin->lock.lock(thread_state);
        /// Retry since the lock was dropped.
        result = bin->mallocNoFreshSlab(thread_state, is_auto, bin_idx);
        if (result == nullptr)
        {
            if (fresh_slab == nullptr)
            {
                /// OOM.
                bin->lock.unlock(thread_state);
                return nullptr;
            }
            result = bin->mallocWithFreshSlab(thread_state, bin_idx, fresh_slab);
            fresh_slab = nullptr;
        }
    }
    if constexpr (config::stats)
    {
        ++bin->stats.num_allocations;
        ++bin->stats.num_requests;
        ++bin->stats.current_regions;
    }
    bin->lock.unlock(thread_state);

    if (fresh_slab != nullptr)
        arenaSlabDeallocate(thread_state, arena, fresh_slab);
    if (zero)
        memset(result, 0, usable_size);
    arenaDecayTick(thread_state, arena);

    return result;
}

/// jemalloc: arena_malloc_hard
void * arenaMallocHard(ThreadState * thread_state, Arena * arena, size_t size, SizeClassIdx idx, bool zero, bool slab)
{
    ALLOCATOR_ASSERT(thread_state != nullptr || arena != nullptr);

    if (ALLOCATOR_LIKELY(thread_state != nullptr))
        arena = arenaChooseMaybeHuge(*thread_state, arena, size);
    if (ALLOCATOR_UNLIKELY(arena == nullptr))
        return nullptr;

    if (ALLOCATOR_LIKELY(slab))
    {
        ALLOCATOR_ASSERT(size_classes::canUseSlab(size));
        return arenaMallocSmall(thread_state, arena, idx, zero);
    }
    else
    {
        return largeMalloc(thread_state, arena, size_classes::sizeToUsableSize(size), zero);
    }
}

/// jemalloc: arena_palloc
void * arenaAllocateAligned(
    ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero, bool slab, ThreadCache * thread_cache)
{
    if (slab)
    {
        ALLOCATOR_ASSERT(size_classes::canUseSlab(usable_size));
        /// Small; alignment doesn't require special slab placement.

        /// usize should be a result of `size_classes::alignedSizeToUsableSize`.
        ALLOCATOR_ASSERT((usable_size & (alignment - 1)) == 0);

        /// Small usize can't come from an alignment larger than a page.
        ALLOCATOR_ASSERT(alignment <= PAGE);

        return arenaMalloc(thread_state, arena, usable_size, size_classes::sizeToIndex(usable_size), zero, slab, thread_cache, true);
    }
    else
    {
        if (ALLOCATOR_LIKELY(alignment <= CACHE_LINE))
            return largeMalloc(thread_state, arena, usable_size, zero);
        else
            return largeAllocateAligned(thread_state, arena, usable_size, alignment, zero);
    }
}

/// --- Small deallocation --------------------------------------------------------------------------------------------

/// jemalloc: arena_dalloc_bin
static void arenaDeallocateBin(ThreadState * thread_state, Arena * arena, Extent * extent, void * ptr)
{
    SizeClassIdx bin_idx = extent->sizeClassIdx();
    unsigned bin_shard = extent->binShard();
    Bin * bin = arenaGetBin(arena, bin_idx, bin_shard);

    bin->lock.lock(thread_state);
    BinDeallocateLockedInfo info;
    Bin::deallocateLockedBegin(info, bin_idx);
    bool result = bin->deallocateLockedStep(thread_state, arenaIsAuto(arena), info, bin_idx, extent, ptr);
    bin->deallocateLockedFinish(thread_state, info);
    bin->lock.unlock(thread_state);

    if (result)
        arenaSlabDeallocate(thread_state, arena, extent);
}

/// jemalloc: arena_dalloc_small
void arenaDeallocateSmall(ThreadState * thread_state, void * ptr)
{
    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    Arena * arena = arenaGetFromExtent(extent);

    arenaDeallocateBin(thread_state, arena, extent, ptr);
    arenaDecayTick(thread_state, arena);
}

/// jemalloc: arena_ptr_array_flush_ptr_getter
static const void * arenaPtrArrayFlushPtrGetter(void * array_context, size_t idx)
{
    CacheBinPtrArray * array = static_cast<CacheBinPtrArray *>(array_context);
    return array->ptr[idx];
}

/// jemalloc: arena_ptr_array_flush_metadata_visitor
static void arenaPtrArrayFlushMetadataVisitor(void * size_class_idx_sum_context, FullAllocContext * alloc_context)
{
    size_t * size_class_idx_sum = static_cast<size_t *>(size_class_idx_sum_context);
    *size_class_idx_sum -= alloc_context->size_class_idx;
    /// util_prefetch_write_range(alloc_ctx->edata, sizeof(edata_t))
    for (size_t i = 0; i < sizeof(Extent); i += CACHE_LINE)
    {
        std::byte * p = reinterpret_cast<std::byte *>(alloc_context->extent) + i;
        if constexpr (config::debug)
            *reinterpret_cast<volatile char *>(p);
        __builtin_prefetch(p, 1, 3);
    }
}

/// jemalloc: arena_ptr_array_flush_size_check_fail
[[maybe_unused]] ALLOCATOR_NOINLINE static void arenaPtrArrayFlushSizeCheckFail(
    CacheBinPtrArray * array, SizeClassIdx size_class_idx, size_t num_ptrs, ExtentMapBatchLookupResult * extents)
{
    [[maybe_unused]] bool found_mismatch = false;
    for (size_t i = 0; i < num_ptrs; ++i)
    {
        SizeClassIdx true_size_class_idx = extents[i].extent->sizeClassIdx();
        if (true_size_class_idx != size_class_idx)
        {
            found_mismatch = true;
            safetyCheckFailSizedDealloc(
                /* current_dealloc */ false,
                /* ptr */ arenaPtrArrayFlushPtrGetter(array, i),
                /* true_size */ size_classes::indexToSize(true_size_class_idx),
                /* input_size */ size_classes::indexToSize(size_class_idx));
        }
    }
    ALLOCATOR_ASSERT(found_mismatch);
}

/// jemalloc: arena_ptr_array_flush_impl_small
ALLOCATOR_ALWAYS_INLINE static void arenaPtrArrayFlushImplSmall(
    ThreadState * thread_state,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    ExtentMapBatchLookupResult * item_extent,
    CacheBinSize num_flush,
    Arena * stats_arena,
    CacheBinStats ** merge_stats)
{
    /// The slabs where we freed the last remaining object in the slab (and so need to free the slab itself).
    unsigned deallocation_count = 0;
    /// VARIABLE_ARRAY(edata_t *, dalloc_slabs, nflush + 1); nflush <= CACHE_BIN_NUM_FLUSH_BATCH_MAX.
    Extent * slabs_to_deallocate[CACHE_BIN_NUM_FLUSH_BATCH_MAX + 1];
    ALLOCATOR_ASSERT(num_flush <= CACHE_BIN_NUM_FLUSH_BATCH_MAX);

    /// We're about to grab a bunch of locks. If one of them happens to be the one guarding the arena-level stats
    /// counters we flush our thread-local ones to, we do so under one critical section.
    ///
    /// We maintain the invariant that all edatas yet to be flushed are contained in the half-open range
    /// [flush_start, flush_end). We'll repeatedly partition the array so that the unflushed items are at the end.
    unsigned flush_start = 0;

    while (flush_start < num_flush)
    {
        /// After our partitioning step, all objects to flush will be in the half-open range
        /// [prev_flush_start, flush_start), and flush_start will be updated to correspond to the next loop iteration.
        unsigned prev_flush_start = flush_start;

        Extent * current_extent = item_extent[flush_start].extent;
        unsigned current_arena_idx = current_extent->arenaIdx();
        Arena * current_arena = arenaGet(thread_state, current_arena_idx, false);

        unsigned current_bin_shard = current_extent->binShard();
        Bin * current_bin = arenaGetBin(current_arena, bin_idx, current_bin_shard);
        ALLOCATOR_ASSERT(current_bin_shard < bin_infos[bin_idx].num_shards);
        /// Start off the partition; item_edata[i] always matches itself of course.
        ++flush_start;
        for (unsigned i = flush_start; i < num_flush; ++i)
        {
            [[maybe_unused]] void * ptr = array->ptr[i];
            Extent * extent = item_extent[i].extent;
            ALLOCATOR_ASSERT(ptr != nullptr && extent != nullptr);
            ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(ptr) >= reinterpret_cast<uintptr_t>(extent->addr()));
            ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(extent->past()));
            if (extent->arenaIdx() == current_arena_idx && extent->binShard() == current_bin_shard)
            {
                /// Swap the edatas.
                ExtentMapBatchLookupResult temp_extent = item_extent[flush_start];
                item_extent[flush_start] = item_extent[i];
                item_extent[i] = temp_extent;
                /// Swap the pointers.
                void * temp_ptr = array->ptr[flush_start];
                array->ptr[flush_start] = array->ptr[i];
                array->ptr[i] = temp_ptr;
                ++flush_start;
            }
        }
        /// Make sure we implemented partitioning correctly.
        if constexpr (config::debug)
        {
            for (unsigned i = prev_flush_start; i < flush_start; ++i)
            {
                Extent * extent = item_extent[i].extent;
                ALLOCATOR_ASSERT(extent->arenaIdx() == current_arena_idx);
                ALLOCATOR_ASSERT(extent->binShard() == current_bin_shard);
            }
            for (unsigned i = flush_start; i < num_flush; ++i)
            {
                Extent * extent = item_extent[i].extent;
                ALLOCATOR_ASSERT(extent->arenaIdx() != current_arena_idx || extent->binShard() != current_bin_shard);
            }
        }

        /// Actually do the flushing.
        current_bin->lock.lock(thread_state);

        /// Flush stats first, if that was the right lock. Note that we don't actually have to flush stats into the
        /// current thread's binshard. Flushing into any binshard in the same arena is enough; we don't expose stats
        /// on per-binshard basis (just per-bin).
        if (config::stats && stats_arena == current_arena && *merge_stats != nullptr)
        {
            ++current_bin->stats.num_flushes;
            current_bin->stats.num_requests += (*merge_stats)->num_requests;
            *merge_stats = nullptr;
        }

        /// Next flush objects.
        BinDeallocateLockedInfo deallocate_bin_info = {};
        Bin::deallocateLockedBegin(deallocate_bin_info, bin_idx);
        for (unsigned i = prev_flush_start; i < flush_start; ++i)
        {
            void * ptr = array->ptr[i];
            Extent * extent = item_extent[i].extent;
            if (current_bin->deallocateLockedStep(thread_state, arenaIsAuto(current_arena), deallocate_bin_info, bin_idx, extent, ptr))
            {
                slabs_to_deallocate[deallocation_count] = extent;
                ++deallocation_count;
            }
        }

        current_bin->deallocateLockedFinish(thread_state, deallocate_bin_info);
        current_bin->lock.unlock(thread_state);

        arenaDecayTicks(thread_state, current_arena, flush_start - prev_flush_start);
    }

    /// Handle all deferred slab dalloc.
    for (unsigned i = 0; i < deallocation_count; ++i)
    {
        Extent * slab = slabs_to_deallocate[i];
        arenaSlabDeallocate(thread_state, arenaGetFromExtent(slab), slab);
    }

    if (config::stats && *merge_stats != nullptr)
    {
        /// The flush loop didn't happen to flush to this thread's arena, so the stats didn't get merged. Manually do
        /// so now.
        Bin * bin = binChoose(thread_state, stats_arena, bin_idx, nullptr);
        bin->lock.lock(thread_state);
        ++bin->stats.num_flushes;
        bin->stats.num_requests += (*merge_stats)->num_requests;
        *merge_stats = nullptr;
        bin->lock.unlock(thread_state);
    }
}

/// jemalloc: arena_ptr_array_flush_impl_large
ALLOCATOR_ALWAYS_INLINE static void arenaPtrArrayFlushImplLarge(
    ThreadState * thread_state,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    ExtentMapBatchLookupResult * item_extent,
    CacheBinSize num_flush,
    Arena * stats_arena,
    CacheBinStats ** merge_stats)
{
    /// We're about to grab a bunch of locks. If one of them happens to be the one guarding the arena-level stats
    /// counters we flush our thread-local ones to, we do so under one critical section.
    while (num_flush > 0)
    {
        /// Lock the arena, or bin, associated with the first object.
        Extent * extent = item_extent[0].extent;
        unsigned current_arena_idx = extent->arenaIdx();
        Arena * current_arena = arenaGet(thread_state, current_arena_idx, false);

        if (!arenaIsAuto(current_arena))
            current_arena->large_mutex.lock(thread_state);

        /// If we acquired the right lock and have some stats to flush, flush them.
        if (config::stats && stats_arena == current_arena && *merge_stats != nullptr)
        {
            arenaStatsLargeFlushNumRequestsAdd(thread_state, &stats_arena->stats, bin_idx, (*merge_stats)->num_requests);
            *merge_stats = nullptr;
        }

        /// Large allocations need special prep done. Afterwards, we can drop the large lock.
        for (unsigned i = 0; i < num_flush; ++i)
        {
            [[maybe_unused]] void * ptr = array->ptr[i];
            extent = item_extent[i].extent;
            ALLOCATOR_ASSERT(ptr != nullptr && extent != nullptr);

            if (extent->arenaIdx() == current_arena_idx)
                largeDeallocatePrepareLocked(thread_state, extent);
        }
        if (!arenaIsAuto(current_arena))
            current_arena->large_mutex.unlock(thread_state);

        /// Deallocate whatever we can.
        unsigned num_deferred = 0;
        for (unsigned i = 0; i < num_flush; ++i)
        {
            void * ptr = array->ptr[i];
            extent = item_extent[i].extent;
            ALLOCATOR_ASSERT(ptr != nullptr && extent != nullptr);
            if (extent->arenaIdx() != current_arena_idx)
            {
                /// The object was allocated either via a different arena, or a different bin in this arena. Either
                /// way, stash the object so that it can be handled in a future pass.
                array->ptr[num_deferred] = ptr;
                item_extent[num_deferred].extent = extent;
                ++num_deferred;
                continue;
            }
            if (largeDeallocateSafetyChecks(extent, ptr, size_classes::indexToSize(bin_idx)))
            {
                /// See the comment in isfree.
                continue;
            }
            largeDeallocateFinish(thread_state, extent);
        }
        arenaDecayTicks(thread_state, current_arena, num_flush - num_deferred);
        num_flush = CacheBinSize(num_deferred);
    }

    if (config::stats && *merge_stats != nullptr)
    {
        arenaStatsLargeFlushNumRequestsAdd(thread_state, &stats_arena->stats, bin_idx, (*merge_stats)->num_requests);
        *merge_stats = nullptr;
    }
}

/// jemalloc: arena_ptr_array_flush_impl
ALLOCATOR_ALWAYS_INLINE static void arenaPtrArrayFlushImpl(
    ThreadState & thread_state,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    unsigned num_flush,
    bool small,
    Arena * stats_arena,
    CacheBinStats ** merge_stats)
{
    ThreadState * thread_state_ptr = &thread_state;
    /// VARIABLE_ARRAY(emap_batch_lookup_result_t, item_edata, nflush + 1): the last element is never touched.
    ExtentMapBatchLookupResult item_extent[CACHE_BIN_NUM_FLUSH_BATCH_MAX + 1];
    ALLOCATOR_ASSERT(num_flush <= CACHE_BIN_NUM_FLUSH_BATCH_MAX);
    /// This gets compiled away when `config_opt_safety_checks` is false. Checks for sized deallocation bugs, failing
    /// early rather than corrupting metadata.
    size_t size_class_idx_sum = size_t(bin_idx) * num_flush;
    arena_extent_map_global.extentLookupBatch(
        thread_state, num_flush, &arenaPtrArrayFlushPtrGetter, array, &arenaPtrArrayFlushMetadataVisitor, &size_class_idx_sum, item_extent);
    if (config::option_safety_checks && ALLOCATOR_UNLIKELY(size_class_idx_sum != 0))
        arenaPtrArrayFlushSizeCheckFail(array, bin_idx, num_flush, item_extent);

    /// The small/large flush logic is very similar; you might conclude that it's a good opportunity to share code.
    /// We've tried this, and by and large found this to obscure more than it helps; there are so many fiddly bits
    /// around things like stats handling, precisely when and which mutexes are acquired, etc., that almost all code
    /// ends up being gated behind 'if (small) { ... } else { ... }'. Even though the '...' is morally equivalent, the
    /// code itself needs slight tweaks.
    if (small)
        arenaPtrArrayFlushImplSmall(thread_state_ptr, bin_idx, array, item_extent, CacheBinSize(num_flush), stats_arena, merge_stats);
    else
        arenaPtrArrayFlushImplLarge(thread_state_ptr, bin_idx, array, item_extent, CacheBinSize(num_flush), stats_arena, merge_stats);
}

/// jemalloc: arena_ptr_array_flush
void arenaPtrArrayFlush(
    ThreadState & thread_state,
    SizeClassIdx bin_idx,
    CacheBinPtrArray * array,
    unsigned num_flush,
    bool small,
    Arena * stats_arena,
    CacheBinStats merge_stats)
{
    ALLOCATOR_ASSERT(array != nullptr && array->ptr != nullptr);
    /// The input cache bin stats represent a snapshot taken when the pointer array is set up, and will be merged into
    /// the next-level bin stats. The original bin stats will be reset by the caller itself. This separation ensures
    /// that each layer operates independently and does not modify another layer's data directly.
    CacheBinStats * stats = &merge_stats;
    unsigned num_flush_batch;
    unsigned num_flushed = 0;
    CacheBinPtrArray ptrs_batch;
    do
    {
        num_flush_batch = num_flush - num_flushed;
        if (num_flush_batch > CACHE_BIN_NUM_FLUSH_BATCH_MAX)
            num_flush_batch = CACHE_BIN_NUM_FLUSH_BATCH_MAX;
        ALLOCATOR_ASSERT(num_flush_batch <= CACHE_BIN_NUM_FLUSH_BATCH_MAX);
        ptrs_batch.n = CacheBinSize(num_flush_batch);
        ptrs_batch.ptr = array->ptr + num_flushed;
        arenaPtrArrayFlushImpl(thread_state, bin_idx, &ptrs_batch, num_flush_batch, small, stats_arena, &stats);
        num_flushed += num_flush_batch;
    } while (num_flushed < num_flush);
    ALLOCATOR_ASSERT(num_flush == num_flushed);
    ALLOCATOR_ASSERT((array->ptr + num_flush) == (ptrs_batch.ptr + num_flush_batch));
    if constexpr (config::stats)
        ALLOCATOR_ASSERT(stats == nullptr);
}

/// --- Reallocation --------------------------------------------------------------------------------------------------

/// jemalloc: arena_ralloc_no_move
bool arenaReallocateNoMove(ThreadState * thread_state, void * ptr, size_t old_size, size_t size, size_t extra, bool zero, size_t * new_size)
{
    bool result;
    /// Calls with non-zero extra had to clamp extra.
    ALLOCATOR_ASSERT(extra == 0 || size + extra <= SIZE_CLASS_LARGE_MAX_CLASS);

    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    if (ALLOCATOR_UNLIKELY(size > SIZE_CLASS_LARGE_MAX_CLASS))
    {
        result = true;
    }
    else
    {
        size_t usable_size_min = size_classes::sizeToUsableSize(size);
        size_t usable_size_max = size_classes::sizeToUsableSize(size + extra);
        if (ALLOCATOR_LIKELY(old_size <= SIZE_CLASS_SMALL_MAX_CLASS && usable_size_min <= SIZE_CLASS_SMALL_MAX_CLASS))
        {
            /// Avoid moving the allocation if the size class can be left the same.
            ALLOCATOR_ASSERT(bin_infos[size_classes::sizeToIndex(old_size)].region_size == old_size);
            if ((usable_size_max > SIZE_CLASS_SMALL_MAX_CLASS
                 || size_classes::sizeToIndex(usable_size_max) != size_classes::sizeToIndex(old_size))
                && (size > old_size || usable_size_max < old_size))
            {
                result = true;
            }
            else
            {
                Arena * arena = arenaGetFromExtent(extent);
                arenaDecayTick(thread_state, arena);
                result = false;
            }
        }
        else if (old_size >= SIZE_CLASS_LARGE_MIN_CLASS && usable_size_max >= SIZE_CLASS_LARGE_MIN_CLASS)
        {
            result = largeReallocateNoMove(thread_state, extent, usable_size_min, usable_size_max, zero);
        }
        else
        {
            result = true;
        }
    }
    /// done:
    ALLOCATOR_ASSERT(extent == arena_extent_map_global.extentLookup(thread_state, ptr));
    *new_size = extent->usableSize();

    return result;
}

/// jemalloc: arena_ralloc_move_helper
static void * arenaReallocateMoveHelper(
    ThreadState * thread_state, Arena * arena, size_t usable_size, size_t alignment, bool zero, bool slab, ThreadCache * thread_cache)
{
    if (alignment == 0)
        return arenaMalloc(thread_state, arena, usable_size, size_classes::sizeToIndex(usable_size), zero, slab, thread_cache, true);
    usable_size = size_classes::alignedSizeToUsableSize(usable_size, alignment);
    if (ALLOCATOR_UNLIKELY(usable_size == 0 || usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return nullptr;
    /// ipalloct_explicit_slab -> ipallocztm_explicit_slab(..., is_internal = false, arena) -> arena_palloc.
    void * result = arenaAllocateAligned(thread_state, arena, usable_size, alignment, zero, slab, thread_cache);
    ALLOCATOR_ASSERT(alignmentAddrToBase(result, alignment) == result);
    return result;
}

/// jemalloc: arena_ralloc
void * arenaReallocate(
    ThreadState * thread_state,
    Arena * arena,
    void * ptr,
    size_t old_size,
    size_t size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * thread_cache)
{
    size_t usable_size = alignment == 0 ? size_classes::sizeToUsableSize(size) : size_classes::alignedSizeToUsableSize(size, alignment);
    if (ALLOCATOR_UNLIKELY(usable_size == 0 || size > SIZE_CLASS_LARGE_MAX_CLASS))
        return nullptr;

    if (ALLOCATOR_LIKELY(slab))
    {
        ALLOCATOR_ASSERT(size_classes::canUseSlab(usable_size));
        /// Try to avoid moving the allocation.
        size_t new_size;
        if (!arenaReallocateNoMove(thread_state, ptr, old_size, usable_size, 0, zero, &new_size))
        {
            /// hook_invoke_expand: hooks are dropped.
            return ptr;
        }
    }

    if (old_size >= SIZE_CLASS_LARGE_MIN_CLASS && usable_size >= SIZE_CLASS_LARGE_MIN_CLASS)
        return largeReallocate(thread_state, arena, ptr, usable_size, alignment, zero, thread_cache);

    /// size and oldsize are different enough that we need to move the object. In that case, fall back to allocating
    /// new space and copying.
    void * result = arenaReallocateMoveHelper(thread_state, arena, usable_size, alignment, zero, slab, thread_cache);
    if (result == nullptr)
        return nullptr;

    /// hook_invoke_alloc, hook_invoke_dalloc: hooks are dropped.

    /// Junk/zero-filling were already done by ipalloc()/arena_malloc().
    size_t copy_size = (usable_size < old_size) ? usable_size : old_size;
    memcpy(result, ptr, copy_size);
    /// isdalloct(tsdn, ptr, oldsize, tcache, NULL, true)
    arenaSizedDeallocate(thread_state, ptr, old_size, thread_cache, nullptr, true);
    return result;
}

/// --- Misc ----------------------------------------------------------------------------------------------------------

/// jemalloc: arena_dss_prec_get
SbrkPrecedence arenaSbrkPrecedenceGet(Arena * arena)
{
    return SbrkPrecedence(arena->sbrk_precedence.load(std::memory_order_acquire));
}

/// jemalloc: arena_dss_prec_set
bool arenaSbrkPrecedenceSet(Arena * arena, SbrkPrecedence sbrk_precedence)
{
    if constexpr (!config::have_sbrk)
        return sbrk_precedence != SbrkPrecedence::Disabled;
    arena->sbrk_precedence.store(unsigned(sbrk_precedence), std::memory_order_release);
    return false;
}

/// jemalloc: arena_name_get
void arenaNameGet(Arena * arena, char * name)
{
    const char * end = static_cast<const char *>(memchr(arena->name, '\0', ARENA_NAME_LEN));
    ALLOCATOR_ASSERT(end != nullptr);
    size_t len = size_t(end - arena->name) + 1;
    ALLOCATOR_ASSERT(len > 0 && len <= ARENA_NAME_LEN);

    strncpy(name, arena->name, len);
}

/// jemalloc: arena_name_set
void arenaNameSet(Arena * arena, const char * name)
{
    strncpy(arena->name, name, ARENA_NAME_LEN);
    arena->name[ARENA_NAME_LEN - 1] = '\0';
}

/// jemalloc: arena_dirty_decay_ms_default_get
ssize_t arenaDirtyDecayMsDefaultGet()
{
    return dirty_decay_ms_default.load(std::memory_order_relaxed);
}

/// jemalloc: arena_dirty_decay_ms_default_set
bool arenaDirtyDecayMsDefaultSet(ssize_t decay_ms)
{
    if (!Decay::msValid(decay_ms))
        return true;
    dirty_decay_ms_default.store(decay_ms, std::memory_order_relaxed);
    return false;
}

/// jemalloc: arena_muzzy_decay_ms_default_get
ssize_t arenaMuzzyDecayMsDefaultGet()
{
    return muzzy_decay_ms_default.load(std::memory_order_relaxed);
}

/// jemalloc: arena_muzzy_decay_ms_default_set
bool arenaMuzzyDecayMsDefaultSet(ssize_t decay_ms)
{
    if (!Decay::msValid(decay_ms))
        return true;
    muzzy_decay_ms_default.store(decay_ms, std::memory_order_relaxed);
    return false;
}

/// jemalloc: arena_retain_grow_limit_get_set
bool arenaRetainGrowLimitGetSet(ThreadState & thread_state, Arena * arena, size_t * old_limit, size_t * new_limit)
{
    ALLOCATOR_ASSERT(options.retain);
    return arena->page_allocator_shard.page_allocator.retainGrowLimitGetSet(&thread_state, old_limit, new_limit);
}

/// --- Creation ------------------------------------------------------------------------------------------------------

/// jemalloc: arena_new
Arena * arenaNew(ThreadState * thread_state, unsigned idx, const ArenaConfig * config)
{
    Base * base;
    if (idx == 0)
    {
        base = base0Get();
    }
    else
    {
        base = Base::create(thread_state, idx, config->extent_hooks_ptr, config->metadata_use_hooks);
        if (base == nullptr)
            return nullptr;
    }

    Arena * arena = nullptr;
    Nanoseconds current_time = Nanoseconds::zero();

    size_t arena_size = alignmentCeiling(sizeof(Arena), CACHE_LINE) + sizeof(Bin) * arena_num_bins_total;
    void * memory = base->alloc(thread_state, arena_size, CACHE_LINE);
    if (memory == nullptr)
        goto label_error;

    /// The memory is zeroed; the constructors only produce the zero state (plus the static mutex initializers).
    arena = new (memory) Arena;
    ALLOCATOR_ASSERT(
        reinterpret_cast<uintptr_t>(arena->allBins() + arena_num_bins_total) <= reinterpret_cast<uintptr_t>(arena) + arena_size);
    arena->num_threads[0].store(0, std::memory_order_relaxed);
    arena->num_threads[1].store(0, std::memory_order_relaxed);
    arena->last_thread = nullptr;

    if constexpr (config::stats)
    {
        /// arena_stats_init: there is no stats mutex, and the memory is zeroed.
        arena->thread_cache_list.init();
        arena->cache_bin_array_descriptor_list.init();
        if (arena->thread_cache_list_mutex.init("tcache_ql", MutexRank::THREAD_CACHE_LIST, MutexLockOrder::RankExclusive))
            goto label_error;
    }

    arena->sbrk_precedence.store(unsigned(extentSbrkPrecedenceGet()), std::memory_order_relaxed);

    arena->large.init();
    if (arena->large_mutex.init("arena_large", MutexRank::ARENA_LARGE, MutexLockOrder::RankExclusive))
        goto label_error;

    current_time.initUpdate();
    if (arena->page_allocator_shard.init(
            thread_state,
            &arena_extent_map_global,
            base,
            idx,
            &arena->stats.page_allocator_shard_stats,
            /* stats_mtx */ nullptr,
            current_time,
            oversize_threshold,
            arenaDirtyDecayMsDefaultGet(),
            arenaMuzzyDecayMsDefaultGet()))
        goto label_error;

    /// Initialize bins.
    arena->bin_shard_next.store(0, std::memory_order_release);
    for (unsigned i = 0; i < arena_num_bins_total; ++i)
    {
        Bin * bin = new (arena->allBins() + i) Bin;
        if (bin->init())
            goto label_error;
    }

    arena->base = base;
    /// jemalloc stores `ind` right after publishing the arena; it is stored first here (the value is the same as
    /// `base->idxGet()`, so the readers cannot observe a difference other than a race on a not yet written field).
    arena->idx = idx;
    /// Set arena before creating background threads.
    arenaSet(idx, arena);

    /// Init the name.
    format(arena->name, sizeof(arena->name), "%s_%u", arenaIsAuto(arena) ? "auto" : "manual", arena->idx);
    arena->name[ARENA_NAME_LEN - 1] = '\0';

    arena->create_time.initUpdate();

    /// HPA is dropped (`opt.hpa` is always false).

    /// We don't support reentrancy for arena 0 bootstrapping.
    if (idx != 0)
    {
        /// If we're here, then arena 0 already exists, so bootstrapping is done enough that we should have tsd.
        ALLOCATOR_ASSERT(thread_state != nullptr);
        preReentrancy(*thread_state, arena);
        /// test_hooks_arena_new_hook: test hooks are dropped.
        postReentrancy(*thread_state);
    }

    return arena;

label_error:
    if (idx != 0)
        base->destroy(thread_state);
    return nullptr;
}

/// jemalloc: arena_create_huge_arena
static Arena * arenaCreateHugeArena(ThreadState & thread_state, unsigned idx)
{
    ALLOCATOR_ASSERT(idx != 0);

    Arena * huge_arena = arenaGet(&thread_state, idx, true);
    if (huge_arena == nullptr)
        return nullptr;

    const char * huge_arena_name = "auto_oversize";
    strncpy(huge_arena->name, huge_arena_name, ARENA_NAME_LEN);
    huge_arena->name[ARENA_NAME_LEN - 1] = '\0';

    /// Purge eagerly for huge allocations, because: 1) number of huge allocations is usually small, which means ticker
    /// based decay is not reliable; and 2) less immediate reuse is expected for huge allocations.
    ///
    /// However, with background threads enabled, keep normal purging since the purging delay is bounded.
    if (!backgroundThreadEnabled() && arenaDirtyDecayMsDefaultGet() > 0)
        arenaDecayMsSet(&thread_state, huge_arena, extent_state_dirty, 0);
    if (!backgroundThreadEnabled() && arenaMuzzyDecayMsDefaultGet() > 0)
        arenaDecayMsSet(&thread_state, huge_arena, extent_state_muzzy, 0);

    return huge_arena;
}

/// jemalloc: arena_choose_huge
Arena * arenaChooseHuge(ThreadState & thread_state)
{
    /// huge_arena_ind can be 0 during init (will use a0).

    Arena * huge_arena = arenaGet(&thread_state, huge_arena_idx, false);
    if (huge_arena == nullptr)
    {
        /// Create the huge arena on demand.
        huge_arena = arenaCreateHugeArena(thread_state, huge_arena_idx);
    }

    return huge_arena;
}

/// jemalloc: arena_init_huge
bool arenaInitHuge(ThreadState * thread_state, Arena * arena0_)
{
    bool huge_enabled;
    ALLOCATOR_ASSERT(huge_arena_idx == 0);

    /// The threshold should be large size class.
    if (options.oversize_threshold > SIZE_CLASS_LARGE_MAX_CLASS || options.oversize_threshold < SIZE_CLASS_LARGE_MIN_CLASS)
    {
        options.oversize_threshold = 0;
        oversize_threshold = SIZE_CLASS_LARGE_MAX_CLASS + PAGE;
        huge_enabled = false;
    }
    else
    {
        /// Reserve the index for the huge arena.
        huge_arena_idx = numArenasTotalGet();
        ALLOCATOR_ASSERT(huge_arena_idx != 0);
        oversize_threshold = options.oversize_threshold;
        /// a0 init happened before the options were parsed.
        arena0_->page_allocator_shard.page_allocator.oversize_threshold.store(oversize_threshold, std::memory_order_relaxed);
        /// Initialize the `huge_arena_transparent_huge_pages` fields under b0's mutex (so that b0's THP auto-switch won't happen
        /// concurrently). `opt.huge_arena_pac_thp` is not ported (off by default): only the locking is kept, because
        /// it is observable through the mutex stats of the base.
        Mutex & b0_mutex = arena0_->base->getMutex();
        b0_mutex.lock(thread_state);
        b0_mutex.unlock(thread_state);
        huge_enabled = true;
    }

    return huge_enabled;
}

/// jemalloc: arena_boot
bool arenaBoot(const SizeClassData * size_class_data, Base * /*base*/, bool /*hpa*/)
{
    arenaDirtyDecayMsDefaultSet(options.dirty_decay_ms);
    arenaMuzzyDecayMsDefaultSet(options.muzzy_decay_ms);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        const SizeClass & size_class = size_class_data->size_class[i];
        arena_bin_idx_division_info[i].init((size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta));
    }

    uint32_t current_offset = uint32_t(sizeof(Arena));
    arena_num_bins_total = 0;
    for (SizeClassIdx i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        arena_bin_offsets[i] = current_offset;
        arena_num_bins_total += bin_infos[i].num_shards;
        current_offset += uint32_t(bin_infos[i].num_shards * sizeof(Bin));
    }
    /// pa_central_init: HPA only.
    return false;
}

/// --- Fork ----------------------------------------------------------------------------------------------------------

/// jemalloc: arena_prefork0
void arenaPrefork0(ThreadState * thread_state, Arena * arena)
{
    arena->page_allocator_shard.prefork0(thread_state);
}

/// jemalloc: arena_prefork1
void arenaPrefork1(ThreadState * thread_state, Arena * arena)
{
    if constexpr (config::stats)
        arena->thread_cache_list_mutex.prefork(thread_state);
}

/// jemalloc: arena_prefork2
void arenaPrefork2(ThreadState * thread_state, Arena * arena)
{
    arena->page_allocator_shard.prefork2(thread_state);
}

/// jemalloc: arena_prefork3
void arenaPrefork3(ThreadState * thread_state, Arena * arena)
{
    arena->page_allocator_shard.prefork3(thread_state);
}

/// jemalloc: arena_prefork4
void arenaPrefork4(ThreadState * thread_state, Arena * arena)
{
    arena->page_allocator_shard.prefork4(thread_state);
}

/// jemalloc: arena_prefork5
void arenaPrefork5(ThreadState * thread_state, Arena * arena)
{
    arena->page_allocator_shard.prefork5(thread_state);
}

/// jemalloc: arena_prefork6
void arenaPrefork6(ThreadState * thread_state, Arena * arena)
{
    arena->base->prefork(thread_state);
}

/// jemalloc: arena_prefork7
void arenaPrefork7(ThreadState * thread_state, Arena * arena)
{
    arena->large_mutex.prefork(thread_state);
}

/// jemalloc: arena_prefork8
void arenaPrefork8(ThreadState * thread_state, Arena * arena)
{
    for (unsigned i = 0; i < arena_num_bins_total; ++i)
        arena->allBins()[i].prefork(thread_state);
}

/// jemalloc: arena_postfork_parent
void arenaPostforkParent(ThreadState * thread_state, Arena * arena)
{
    for (unsigned i = 0; i < arena_num_bins_total; ++i)
        arena->allBins()[i].postforkParent(thread_state);

    arena->large_mutex.postforkParent(thread_state);
    arena->base->postforkParent(thread_state);
    arena->page_allocator_shard.postforkParent(thread_state);
    if constexpr (config::stats)
        arena->thread_cache_list_mutex.postforkParent(thread_state);
}

/// jemalloc: arena_postfork_child
void arenaPostforkChild(ThreadState * thread_state_ptr, Arena * arena)
{
    ThreadState & thread_state = *thread_state_ptr;
    arena->num_threads[0].store(0, std::memory_order_relaxed);
    arena->num_threads[1].store(0, std::memory_order_relaxed);
    if (thread_state.arena == arena)
        arenaNumThreadsIncrement(arena, false);
    if (thread_state.internal_arena == arena)
        arenaNumThreadsIncrement(arena, true);
    if constexpr (config::stats)
    {
        arena->thread_cache_list.init();
        arena->cache_bin_array_descriptor_list.init();
        ThreadCacheSlow * thread_cache_slow = threadCacheSlowGet(thread_state);
        if (thread_cache_slow != nullptr && thread_cache_slow->arena == arena)
        {
            ThreadCache * thread_cache = thread_cache_slow->thread_cache;
            arena->thread_cache_list.elementInit(thread_cache_slow);
            arena->thread_cache_list.tailInsert(thread_cache_slow);
            thread_cache_slow->cache_bin_array_descriptor.init(thread_cache->bins);
            arena->cache_bin_array_descriptor_list.tailInsert(&thread_cache_slow->cache_bin_array_descriptor);
        }
    }

    for (unsigned i = 0; i < arena_num_bins_total; ++i)
        arena->allBins()[i].postforkChild(thread_state_ptr);

    arena->large_mutex.postforkChild(thread_state_ptr);
    arena->base->postforkChild(thread_state_ptr);
    arena->page_allocator_shard.postforkChild(thread_state_ptr);
    if constexpr (config::stats)
        arena->thread_cache_list_mutex.postforkChild(thread_state_ptr);
}

}
