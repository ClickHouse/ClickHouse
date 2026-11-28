/// `stats.*`, `stats.arenas.<i>.*`, `approximate_stats.*` (jemalloc: `ctl.c`).
///
/// The values are the snapshot taken by the last `epoch` refresh (`mallctlRefresh`), except `stats.zero_reallocs` and
/// `approximate_stats.active`, which are live. The mutex profiling leaves are generated in MallctlTree.cpp from the
/// accessors defined here; the index function of `stats.arenas` is in Mallctl.cpp, the constant index functions of
/// `bins`, `lextents`, `extents` and `hpa_shard.nonfull_slabs` are in MallctlTree.cpp.

#include <allocator/MallctlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/Options.h>
#include <allocator/Profiling.h>
#include <allocator/ThreadState.h>

#include <atomic>

namespace jemalloc
{

/// The number of `realloc(ptr, 0)` calls (`stats.zero_reallocs`). Defined by the front-end (API.cpp).
/// jemalloc: zero_realloc_count
extern std::atomic<size_t> zero_realloc_count;

}

namespace jemalloc::mallctl
{

namespace
{

constexpr bool statsEnabled()
{
    return config::stats;
}

}

/// --- stats.* ---------------------------------------------------------------------------------------------------------

/// jemalloc: CTL_RO_CGEN(config_stats, stats_*, ctl_stats->*, ...)
#define ALLOCATOR_MALLCTL_STATS_LEAF(name, type, expr) \
    int name( \
        ThreadState & thread_state, \
        const size_t * numeric_path, \
        size_t numeric_path_length, \
        void * old_value, \
        size_t * old_length_ptr, \
        void * new_value, \
        size_t new_length) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [] { return expr; }>( \
            thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length); \
    }

ALLOCATOR_MALLCTL_STATS_LEAF(statsAllocated, size_t, mallctl_stats->allocated)
ALLOCATOR_MALLCTL_STATS_LEAF(statsActive, size_t, mallctl_stats->active)
ALLOCATOR_MALLCTL_STATS_LEAF(statsMetadata, size_t, mallctl_stats->metadata)
ALLOCATOR_MALLCTL_STATS_LEAF(statsMetadataExtent, size_t, mallctl_stats->metadata_extent)
ALLOCATOR_MALLCTL_STATS_LEAF(statsMetadataRadixTree, size_t, mallctl_stats->metadata_radix_tree)
ALLOCATOR_MALLCTL_STATS_LEAF(statsMetadataTransparentHugePages, size_t, mallctl_stats->metadata_transparent_huge_pages)
ALLOCATOR_MALLCTL_STATS_LEAF(statsResident, size_t, mallctl_stats->resident)
ALLOCATOR_MALLCTL_STATS_LEAF(statsMapped, size_t, mallctl_stats->mapped)
ALLOCATOR_MALLCTL_STATS_LEAF(statsRetained, size_t, mallctl_stats->retained)

ALLOCATOR_MALLCTL_STATS_LEAF(statsBackgroundThreadNumThreads, size_t, mallctl_stats->background_thread.num_threads)
ALLOCATOR_MALLCTL_STATS_LEAF(statsBackgroundThreadNumRuns, uint64_t, mallctl_stats->background_thread.num_runs)
ALLOCATOR_MALLCTL_STATS_LEAF(statsBackgroundThreadRunInterval, uint64_t, mallctl_stats->background_thread.run_interval.ns())

#undef ALLOCATOR_MALLCTL_STATS_LEAF

namespace
{

/// jemalloc: MUTEX_PROF_RESET
void mutexProfilingReset(ThreadState * thread_state, Mutex & mutex)
{
    MutexLock lock(thread_state, mutex);
    mutex.profilingDataReset(thread_state);
}

}

/// Resets all mutex stats, including global, arena and bin mutexes. No access checks.
/// jemalloc: stats_mutexes_reset_ctl
int statsMutexesReset(ThreadState & thread_state, const size_t *, size_t, void *, size_t *, void *, size_t)
{
    if constexpr (!config::stats)
        return ENOENT;

    ThreadState * thread_state_ptr = &thread_state;

    /// Global mutexes: ctl and prof.
    mutexProfilingReset(thread_state_ptr, mallctl_mutex);
    if constexpr (config::background_thread)
        mutexProfilingReset(thread_state_ptr, background_thread_lock);
    if (config::profiling && options.profiling)
    {
        mutexProfilingReset(thread_state_ptr, backtrace_to_global_context_mutex);
        mutexProfilingReset(thread_state_ptr, all_thread_data_mutex);
        mutexProfilingReset(thread_state_ptr, profiling_dump_mutex);
        mutexProfilingReset(thread_state_ptr, profiling_recent_alloc_mutex);
        mutexProfilingReset(thread_state_ptr, profiling_recent_dump_mutex);
        mutexProfilingReset(thread_state_ptr, profiling_stats_mutex);
    }

    /// Per arena mutexes.
    unsigned n = numArenasTotalGet();
    for (unsigned i = 0; i < n; ++i)
    {
        Arena * arena = arenaGet(thread_state_ptr, i, false);
        if (arena == nullptr)
            continue;
        mutexProfilingReset(thread_state_ptr, arena->large_mutex);
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.extent_pool.getMutex());
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.page_allocator.extent_cache_dirty.mutex);
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.page_allocator.extent_cache_muzzy.mutex);
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.page_allocator.extent_cache_retained.mutex);
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.page_allocator.decay_dirty.mutex);
        mutexProfilingReset(thread_state_ptr, arena->page_allocator_shard.page_allocator.decay_muzzy.mutex);
        mutexProfilingReset(thread_state_ptr, arena->thread_cache_list_mutex);
        mutexProfilingReset(thread_state_ptr, arena->base->getMutex());

        for (SizeClassIdx j = 0; j < SIZE_CLASS_NUM_BINS; ++j)
        {
            for (unsigned k = 0; k < bin_infos[j].num_shards; ++k)
                mutexProfilingReset(thread_state_ptr, arenaGetBin(arena, j, k)->lock);
        }
    }
    return 0;
}

/// jemalloc: CTL_RO_CGEN(config_stats, stats_zero_reallocs, atomic_load_zu(&zero_realloc_count, ATOMIC_RELAXED), size_t)
int statsZeroReallocs(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyLockedIf<size_t, statsEnabled, [] { return zero_realloc_count.load(std::memory_order_relaxed); }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// Live (not the epoch snapshot): the sum of the active pages of all arenas. It should not be compared with other
/// stats.
/// jemalloc: approximate_stats_active_ctl
int approximateStatsActive(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;

    ThreadState * thread_state_ptr = &thread_state;
    unsigned n = numArenasTotalGet();
    size_t approximate_num_active = 0;
    for (unsigned i = 0; i < n; ++i)
    {
        Arena * arena = arenaGet(thread_state_ptr, i, false);
        if (arena == nullptr)
            continue;
        /// Accumulate nactive pages from each arena's pa_shard.
        approximate_num_active += arena->page_allocator_shard.numActiveGet();
    }

    size_t approximate_active_bytes = approximate_num_active << LOG2_PAGE;
    return read(old_value, old_length_ptr, approximate_active_bytes);
}

/// --- stats.arenas.<i>.* -------------------------------------------------------------------------------------------

/// The basic fields of the slot (`ctl_arena_t`), under `mallctl_mutex`. jemalloc: CTL_RO_GEN(stats_arenas_i_*,
/// arenas_i(numeric_path[2])->*, ...)
#define ALLOCATOR_MALLCTL_ARENA_LEAF(name, type, field) \
    int name( \
        ThreadState & thread_state, \
        const size_t * numeric_path, \
        size_t numeric_path_length, \
        void * old_value, \
        size_t * old_length_ptr, \
        void * new_value, \
        size_t new_length) \
    { \
        return readOnlyLocked<type, [](const size_t * m) { return arenasI(m[2])->field; }>( \
            thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length); \
    }

ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasISbrk, const char *, sbrk)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasIDirtyDecayMs, ssize_t, dirty_decay_ms)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasIMuzzyDecayMs, ssize_t, muzzy_decay_ms)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasINumThreads, unsigned, num_threads)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasIActivePages, size_t, active_pages)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasIDirtyPages, size_t, dirty_pages)
ALLOCATOR_MALLCTL_ARENA_LEAF(statsArenasIMuzzyPages, size_t, muzzy_pages)

#undef ALLOCATOR_MALLCTL_ARENA_LEAF

/// The aggregate small stats of the slot (`ctl_arena_stats_t`). jemalloc: CTL_RO_CGEN(config_stats,
/// stats_arenas_i_small_*, arenas_i(numeric_path[2])->astats->*_small, ...)
#define ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(name, type, field) \
    int name( \
        ThreadState & thread_state, \
        const size_t * numeric_path, \
        size_t numeric_path_length, \
        void * old_value, \
        size_t * old_length_ptr, \
        void * new_value, \
        size_t new_length) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [](const size_t * m) { return arenasI(m[2])->arena_stats->field; }>( \
            thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length); \
    }

ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallAllocated, size_t, allocated_small)
ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallNumAllocations, uint64_t, num_allocations_small)
ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallNumDeallocations, uint64_t, num_deallocations_small)
ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallNumRequests, uint64_t, num_requests_small)
ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallNumFills, uint64_t, num_fills_small)
ALLOCATOR_MALLCTL_ARENA_STATS_LEAF(statsArenasISmallNumFlushes, uint64_t, num_flushes_small)

#undef ALLOCATOR_MALLCTL_ARENA_STATS_LEAF

namespace
{

/// `arenas_i(numeric_path[2])->arena_stats` (requires `mallctl_mutex`).
MallctlArenaStats * slotStats(const size_t * numeric_path)
{
    return arenasI(numeric_path[2])->arena_stats;
}

/// `arena_stats->bin_stats[numeric_path[4]]`. The index function accepts `j == SIZE_CLASS_NUM_BINS` (jemalloc compatibility), which reads the
/// memory that follows the array (the beginning of `large_stats`), as jemalloc does: the address is computed from the
/// beginning of the structure.
const BinStatsData & slotBin(const size_t * numeric_path)
{
    const MallctlArenaStats * arena_stats = slotStats(numeric_path);
    return *reinterpret_cast<const BinStatsData *>(
        reinterpret_cast<const char *>(arena_stats) + offsetof(MallctlArenaStats, bin_stats) + numeric_path[4] * sizeof(BinStatsData));
}

/// `arena_stats->large_stats[numeric_path[4]]` (`j == SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS` reads the beginning of `extent_stats`, see above).
const ArenaStatsLarge & slotLarge(const size_t * numeric_path)
{
    const MallctlArenaStats * arena_stats = slotStats(numeric_path);
    return *reinterpret_cast<const ArenaStatsLarge *>(
        reinterpret_cast<const char *>(arena_stats) + offsetof(MallctlArenaStats, large_stats) + numeric_path[4] * sizeof(ArenaStatsLarge));
}

/// `arena_stats->extent_stats[numeric_path[4]]`.
const PageAllocatorExtentStats & slotExtents(const size_t * numeric_path)
{
    return slotStats(numeric_path)->extent_stats[numeric_path[4]];
}

const PageAllocatorStats & slotPageAllocator(const size_t * numeric_path)
{
    return slotStats(numeric_path)->arena_stats.page_allocator_shard_stats.page_allocator_stats;
}

}

/// jemalloc: CTL_RO_CGEN(config_stats, stats_arenas_i_*, <expr of numeric_path>, type)
#define ALLOCATOR_MALLCTL_SLOT_LEAF(name, type, expr) \
    int name( \
        ThreadState & thread_state, \
        const size_t * numeric_path, \
        size_t numeric_path_length, \
        void * old_value, \
        size_t * old_length_ptr, \
        void * new_value, \
        size_t new_length) \
    { \
        return readOnlyLockedIf<type, statsEnabled, [](const size_t * m) -> type { return expr; }>( \
            thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length); \
    }

/// jemalloc: CTL_RO_GEN(stats_arenas_i_uptime, nstime_ns(&arenas_i(numeric_path[2])->astats->astats.uptime), uint64_t)
int statsArenasIUptime(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyLocked<uint64_t, [](const size_t * m) { return slotStats(m)->arena_stats.uptime.ns(); }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMapped, size_t, slotStats(m)->arena_stats.mapped)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIRetained, size_t, slotPageAllocator(m).retained)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentAvailable, size_t, slotStats(m)->arena_stats.page_allocator_shard_stats.extent_available)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIDirtyNumPurge, uint64_t, slotPageAllocator(m).decay_dirty.num_purge.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIDirtyNumMadvises, uint64_t, slotPageAllocator(m).decay_dirty.num_madvises.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIDirtyPurged, uint64_t, slotPageAllocator(m).decay_dirty.purged.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMuzzyNumPurge, uint64_t, slotPageAllocator(m).decay_muzzy.num_purge.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMuzzyNumMadvises, uint64_t, slotPageAllocator(m).decay_muzzy.num_madvises.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMuzzyPurged, uint64_t, slotPageAllocator(m).decay_muzzy.purged.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBase, size_t, slotStats(m)->arena_stats.base)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIInternal, size_t, slotStats(m)->arena_stats.internal.load(std::memory_order_relaxed))
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMetadataExtent, size_t, slotStats(m)->arena_stats.metadata_extent)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMetadataRadixTree, size_t, slotStats(m)->arena_stats.metadata_radix_tree)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIMetadataTransparentHugePages, size_t, slotStats(m)->arena_stats.metadata_transparent_huge_pages)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIThreadCacheBytes, size_t, slotStats(m)->arena_stats.thread_cache_bytes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIThreadCacheStashedBytes, size_t, slotStats(m)->arena_stats.thread_cache_stashed_bytes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIResident, size_t, slotStats(m)->arena_stats.resident)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIAbandonedVM, size_t, slotPageAllocator(m).abandoned_vm.load(std::memory_order_relaxed))

ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeAllocated, size_t, slotStats(m)->arena_stats.allocated_large)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeNumAllocations, uint64_t, slotStats(m)->arena_stats.num_allocations_large)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeNumDeallocations, uint64_t, slotStats(m)->arena_stats.num_deallocations_large)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeNumRequests, uint64_t, slotStats(m)->arena_stats.num_requests_large)
/// Note: "nmalloc_large" here instead of "nfills" in the read. This is intentional (large has no batch fill).
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeNumFills, uint64_t, slotStats(m)->arena_stats.num_allocations_large)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeNumFlushes, uint64_t, slotStats(m)->arena_stats.num_flushes_large)

ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumAllocations, uint64_t, slotBin(m).stats_data.num_allocations)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumDeallocations, uint64_t, slotBin(m).stats_data.num_deallocations)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumRequests, uint64_t, slotBin(m).stats_data.num_requests)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJCurrentRegions, size_t, slotBin(m).stats_data.current_regions)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumFills, uint64_t, slotBin(m).stats_data.num_fills)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumFlushes, uint64_t, slotBin(m).stats_data.num_flushes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumSlabs, uint64_t, slotBin(m).stats_data.num_slabs)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNumSlabChanges, uint64_t, slotBin(m).stats_data.slab_changes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJCurrentSlabs, size_t, slotBin(m).stats_data.current_slabs)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIBinsJNonFullSlabs, size_t, slotBin(m).stats_data.non_full_slabs)

ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeExtentsJNumAllocations, uint64_t, slotLarge(m).num_allocations.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeExtentsJNumDeallocations, uint64_t, slotLarge(m).num_deallocations.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeExtentsJNumRequests, uint64_t, slotLarge(m).num_requests.readUnsynchronized())
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasILargeExtentsJCurrentLargeExtents, size_t, slotLarge(m).current_large_extents)

ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJNumDirty, size_t, slotExtents(m).num_dirty)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJNumMuzzy, size_t, slotExtents(m).num_muzzy)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJNumRetained, size_t, slotExtents(m).num_retained)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJDirtyBytes, size_t, slotExtents(m).dirty_bytes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJMuzzyBytes, size_t, slotExtents(m).muzzy_bytes)
ALLOCATOR_MALLCTL_SLOT_LEAF(statsArenasIExtentsJRetainedBytes, size_t, slotExtents(m).retained_bytes)

#undef ALLOCATOR_MALLCTL_SLOT_LEAF

/// --- Mutex profiling accessors ---------------------------------------------------------------------------------------

/// `&arenas_i(numeric_path[2])->arena_stats->arena_stats.mutex_profiling_data[idx]`.
const MutexProfilingData * arenaMutexProfilingData(const size_t * numeric_path, unsigned idx)
{
    return &slotStats(numeric_path)->arena_stats.mutex_profiling_data[idx];
}

/// `&arenas_i(numeric_path[2])->arena_stats->bin_stats[numeric_path[4]].mutex_data`.
const MutexProfilingData * binMutexProfilingData(const size_t * numeric_path)
{
    return &slotBin(numeric_path).mutex_data;
}

}
