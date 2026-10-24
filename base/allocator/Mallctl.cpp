/// The `mallctl` machinery (jemalloc: `ctl.c`): name and numeric path lookup, the entry points, the ctl state (`mallctl_mutex`,
/// `mallctl_stats`, `mallctl_arenas`, lazy initialization and the `epoch` refresh) and the leaves and index functions that
/// depend only on the ctl state.

#include <allocator/Mallctl.h>

#include <allocator/Arenas.h>
#include <allocator/Base.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Format.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Options.h>
#include <allocator/Profiling.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstdint>
#include <cstring>

namespace jemalloc
{

/// --- ctl state -------------------------------------------------------------------------------------------------------

constinit Mutex mallctl_mutex;
constinit MallctlStats * mallctl_stats = nullptr;
constinit MallctlArenas * mallctl_arenas = nullptr;

namespace
{

/// Checked without the lock by every entry point, then again under `mallctl_mutex` by `mallctlInit`.
/// jemalloc: ctl_initialized
constinit std::atomic<bool> mallctl_initialized{false};

/// jemalloc: JEMALLOC_VERSION (`jemalloc_macros.h`)
constexpr const char * JEMALLOC_VERSION = "5.3-RC";

}

/// jemalloc: arenas_i2a_impl
unsigned arenasI2aImpl(size_t i, bool compat, bool validate)
{
    switch (i)
    {
        case MALLCTL_ARENAS_ALL: return 0;
        case MALLCTL_ARENAS_DESTROYED: return 1;
        default:
            if (compat && i == mallctl_arenas->num_arenas)
            {
                /// Provide deprecated backward compatibility for accessing the merged stats at index narenas rather
                /// than via MALLCTL_ARENAS_ALL. This is scheduled for removal in 6.0.0.
                return 0;
            }
            if (validate && i >= mallctl_arenas->num_arenas)
                return UINT_MAX;
            /// This function should never be called for an index more than one past the range of indices that have
            /// initialized ctl data.
            ALLOCATOR_ASSERT(i < mallctl_arenas->num_arenas || (!validate && i == mallctl_arenas->num_arenas));
            return static_cast<unsigned>(i) + 2;
    }
}

/// jemalloc: arenas_i_impl
MallctlArena * arenasIImpl(ThreadState * thread_state, size_t i, bool compat, bool init)
{
    ALLOCATOR_ASSERT(!compat || !init);

    MallctlArena * result = mallctl_arenas->arenas[arenasI2aImpl(i, compat, false)];
    if (init && result == nullptr)
    {
        /// The slot and its stats are allocated together (jemalloc: `struct container_s`).
        struct Container
        {
            MallctlArena mallctl_arena;
            MallctlArenaStats arena_stats;
        };
        auto * container = static_cast<Container *>(base0Get()->alloc(thread_state, sizeof(Container), QUANTUM));
        if (container == nullptr)
            return nullptr;
        result = &container->mallctl_arena;
        result->arena_stats = &container->arena_stats;
        result->arena_idx = static_cast<unsigned>(i);
        mallctl_arenas->arenas[arenasI2aImpl(i, compat, false)] = result;
    }

    ALLOCATOR_ASSERT(result == nullptr || arenasI2a(result->arena_idx) == arenasI2a(i));
    return result;
}

/// jemalloc: arenas_i
MallctlArena * arenasI(size_t i)
{
    MallctlArena * result = arenasIImpl(nullptr, i, true, false);
    ALLOCATOR_ASSERT(result != nullptr);
    return result;
}

/// jemalloc: ctl_arena_clear
void mallctlArenaClear(MallctlArena * mallctl_arena)
{
    mallctl_arena->num_threads = 0;
    mallctl_arena->sbrk = sbrk_precedence_names[unsigned(SbrkPrecedence::Limit)];
    mallctl_arena->dirty_decay_ms = -1;
    mallctl_arena->muzzy_decay_ms = -1;
    mallctl_arena->active_pages = 0;
    mallctl_arena->dirty_pages = 0;
    mallctl_arena->muzzy_pages = 0;
    if constexpr (config::stats)
        std::memset(static_cast<void *>(mallctl_arena->arena_stats), 0, sizeof(*mallctl_arena->arena_stats));
}

/// jemalloc: ctl_arenas_i_verify
bool mallctlArenasIVerify(size_t i)
{
    unsigned a = arenasI2aImpl(i, true, true);
    return a == UINT_MAX || !mallctl_arenas->arenas[a]->initialized;
}

namespace
{

/// jemalloc: ctl_background_thread_stats_read
void mallctlBackgroundThreadStatsRead(ThreadState * thread_state)
{
    BackgroundThreadStats * stats = &mallctl_stats->background_thread;
    if (!config::background_thread || backgroundThreadStatsRead(thread_state, stats))
    {
        std::memset(static_cast<void *>(stats), 0, sizeof(BackgroundThreadStats));
        stats->run_interval.initZero();
    }
    mallctl_stats->mutex_profiling_data[global_profiling_mutex_max_per_background_thread].copyFrom(
        stats->max_counter_per_background_thread);
}

}

namespace
{

/// Sets `*dst += *src` non-atomically. This is safe, since everything is synchronized by the ctl mutex.
/// jemalloc: ctl_accum_locked_u64
void mallctlAccumulatedLockedU64(LockedU64 & dst, const LockedU64 & src)
{
    dst.incrementUnsynchronized(src.readUnsynchronized());
}

/// jemalloc: ctl_accum_atomic_zu
void mallctlAccumulatedAtomicSize(std::atomic<size_t> & dst, const std::atomic<size_t> & src)
{
    size_t current_dst = dst.load(std::memory_order_relaxed);
    size_t current_src = src.load(std::memory_order_relaxed);
    dst.store(current_dst + current_src, std::memory_order_relaxed);
}

/// jemalloc: ctl_arena_stats_amerge
void mallctlArenaStatsArenaMerge(ThreadState * thread_state, MallctlArena * mallctl_arena, Arena * arena)
{
    if constexpr (config::stats)
    {
        MallctlArenaStats * arena_stats = mallctl_arena->arena_stats;
        arenaStatsMerge(
            thread_state,
            arena,
            &mallctl_arena->num_threads,
            &mallctl_arena->sbrk,
            &mallctl_arena->dirty_decay_ms,
            &mallctl_arena->muzzy_decay_ms,
            &mallctl_arena->active_pages,
            &mallctl_arena->dirty_pages,
            &mallctl_arena->muzzy_pages,
            &arena_stats->arena_stats,
            arena_stats->bin_stats,
            arena_stats->large_stats,
            arena_stats->extent_stats);

        for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        {
            const BinStats & bin_stats = arena_stats->bin_stats[i].stats_data;
            arena_stats->allocated_small += bin_stats.current_regions * size_classes::indexToSize(i);
            arena_stats->num_allocations_small += bin_stats.num_allocations;
            arena_stats->num_deallocations_small += bin_stats.num_deallocations;
            arena_stats->num_requests_small += bin_stats.num_requests;
            arena_stats->num_fills_small += bin_stats.num_fills;
            arena_stats->num_flushes_small += bin_stats.num_flushes;
        }
    }
    else
    {
        arenaBasicStatsMerge(
            thread_state,
            arena,
            &mallctl_arena->num_threads,
            &mallctl_arena->sbrk,
            &mallctl_arena->dirty_decay_ms,
            &mallctl_arena->muzzy_decay_ms,
            &mallctl_arena->active_pages,
            &mallctl_arena->dirty_pages,
            &mallctl_arena->muzzy_pages);
    }
}

/// jemalloc: ctl_arena_stats_sdmerge
void mallctlArenaStatsSummedDestroyedMerge(MallctlArena * mallctl_summed_destroyed_arena, MallctlArena * mallctl_arena, bool destroyed)
{
    if (!destroyed)
    {
        mallctl_summed_destroyed_arena->num_threads += mallctl_arena->num_threads;
        mallctl_summed_destroyed_arena->active_pages += mallctl_arena->active_pages;
        mallctl_summed_destroyed_arena->dirty_pages += mallctl_arena->dirty_pages;
        mallctl_summed_destroyed_arena->muzzy_pages += mallctl_arena->muzzy_pages;
    }
    else
    {
        ALLOCATOR_ASSERT(mallctl_arena->num_threads == 0);
        ALLOCATOR_ASSERT(mallctl_arena->active_pages == 0);
        ALLOCATOR_ASSERT(mallctl_arena->dirty_pages == 0);
        ALLOCATOR_ASSERT(mallctl_arena->muzzy_pages == 0);
    }

    if constexpr (config::stats)
    {
        MallctlArenaStats * summed_destroyed_stats = mallctl_summed_destroyed_arena->arena_stats;
        MallctlArenaStats * arena_stats = mallctl_arena->arena_stats;
        PageAllocatorStats & summed_destroyed_page_allocator_stats
            = summed_destroyed_stats->arena_stats.page_allocator_shard_stats.page_allocator_stats;
        PageAllocatorStats & arena_page_allocator_stats = arena_stats->arena_stats.page_allocator_shard_stats.page_allocator_stats;

        if (!destroyed)
        {
            summed_destroyed_stats->arena_stats.mapped += arena_stats->arena_stats.mapped;
            summed_destroyed_page_allocator_stats.retained += arena_page_allocator_stats.retained;
            summed_destroyed_stats->arena_stats.page_allocator_shard_stats.extent_available
                += arena_stats->arena_stats.page_allocator_shard_stats.extent_available;
        }

        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_dirty.num_purge, arena_page_allocator_stats.decay_dirty.num_purge);
        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_dirty.num_madvises, arena_page_allocator_stats.decay_dirty.num_madvises);
        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_dirty.purged, arena_page_allocator_stats.decay_dirty.purged);

        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_muzzy.num_purge, arena_page_allocator_stats.decay_muzzy.num_purge);
        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_muzzy.num_madvises, arena_page_allocator_stats.decay_muzzy.num_madvises);
        mallctlAccumulatedLockedU64(
            summed_destroyed_page_allocator_stats.decay_muzzy.purged, arena_page_allocator_stats.decay_muzzy.purged);

        for (unsigned i = 0; i < mutex_profiling_num_arena_mutexes; ++i)
            summed_destroyed_stats->arena_stats.mutex_profiling_data[i].merge(arena_stats->arena_stats.mutex_profiling_data[i]);

        if (!destroyed)
        {
            summed_destroyed_stats->arena_stats.base += arena_stats->arena_stats.base;
            summed_destroyed_stats->arena_stats.metadata_extent += arena_stats->arena_stats.metadata_extent;
            summed_destroyed_stats->arena_stats.metadata_radix_tree += arena_stats->arena_stats.metadata_radix_tree;
            summed_destroyed_stats->arena_stats.resident += arena_stats->arena_stats.resident;
            summed_destroyed_stats->arena_stats.metadata_transparent_huge_pages += arena_stats->arena_stats.metadata_transparent_huge_pages;
            mallctlAccumulatedAtomicSize(summed_destroyed_stats->arena_stats.internal, arena_stats->arena_stats.internal);
        }
        else
        {
            ALLOCATOR_ASSERT(arena_stats->arena_stats.internal.load(std::memory_order_relaxed) == 0);
        }

        if (!destroyed)
            summed_destroyed_stats->allocated_small += arena_stats->allocated_small;
        else
            ALLOCATOR_ASSERT(arena_stats->allocated_small == 0);
        summed_destroyed_stats->num_allocations_small += arena_stats->num_allocations_small;
        summed_destroyed_stats->num_deallocations_small += arena_stats->num_deallocations_small;
        summed_destroyed_stats->num_requests_small += arena_stats->num_requests_small;
        summed_destroyed_stats->num_fills_small += arena_stats->num_fills_small;
        summed_destroyed_stats->num_flushes_small += arena_stats->num_flushes_small;

        if (!destroyed)
            summed_destroyed_stats->arena_stats.allocated_large += arena_stats->arena_stats.allocated_large;
        else
            ALLOCATOR_ASSERT(arena_stats->arena_stats.allocated_large == 0);
        summed_destroyed_stats->arena_stats.num_allocations_large += arena_stats->arena_stats.num_allocations_large;
        summed_destroyed_stats->arena_stats.num_deallocations_large += arena_stats->arena_stats.num_deallocations_large;
        summed_destroyed_stats->arena_stats.num_requests_large += arena_stats->arena_stats.num_requests_large;
        summed_destroyed_stats->arena_stats.num_flushes_large += arena_stats->arena_stats.num_flushes_large;
        mallctlAccumulatedAtomicSize(summed_destroyed_page_allocator_stats.abandoned_vm, arena_page_allocator_stats.abandoned_vm);

        summed_destroyed_stats->arena_stats.thread_cache_bytes += arena_stats->arena_stats.thread_cache_bytes;
        summed_destroyed_stats->arena_stats.thread_cache_stashed_bytes += arena_stats->arena_stats.thread_cache_stashed_bytes;

        if (mallctl_arena->arena_idx == 0)
            summed_destroyed_stats->arena_stats.uptime = arena_stats->arena_stats.uptime;

        /// Merge bin stats.
        for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        {
            const BinStats & bin_stats = arena_stats->bin_stats[i].stats_data;
            BinStats & merged = summed_destroyed_stats->bin_stats[i].stats_data;
            merged.num_allocations += bin_stats.num_allocations;
            merged.num_deallocations += bin_stats.num_deallocations;
            merged.num_requests += bin_stats.num_requests;
            if (!destroyed)
                merged.current_regions += bin_stats.current_regions;
            else
                ALLOCATOR_ASSERT(bin_stats.current_regions == 0);
            merged.num_fills += bin_stats.num_fills;
            merged.num_flushes += bin_stats.num_flushes;
            merged.num_slabs += bin_stats.num_slabs;
            merged.slab_changes += bin_stats.slab_changes;
            if (!destroyed)
            {
                merged.current_slabs += bin_stats.current_slabs;
                merged.non_full_slabs += bin_stats.non_full_slabs;
            }
            else
            {
                ALLOCATOR_ASSERT(bin_stats.current_slabs == 0);
                ALLOCATOR_ASSERT(bin_stats.non_full_slabs == 0);
            }
            summed_destroyed_stats->bin_stats[i].mutex_data.merge(arena_stats->bin_stats[i].mutex_data);
        }

        /// Merge stats for large allocations.
        for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS; ++i)
        {
            mallctlAccumulatedLockedU64(
                summed_destroyed_stats->large_stats[i].num_allocations, arena_stats->large_stats[i].num_allocations);
            mallctlAccumulatedLockedU64(
                summed_destroyed_stats->large_stats[i].num_deallocations, arena_stats->large_stats[i].num_deallocations);
            mallctlAccumulatedLockedU64(summed_destroyed_stats->large_stats[i].num_requests, arena_stats->large_stats[i].num_requests);
            if (!destroyed)
                summed_destroyed_stats->large_stats[i].current_large_extents += arena_stats->large_stats[i].current_large_extents;
            else
                ALLOCATOR_ASSERT(arena_stats->large_stats[i].current_large_extents == 0);
        }

        /// Merge extents stats.
        for (unsigned i = 0; i < SIZE_CLASS_NUM_PAGE_SIZES; ++i)
        {
            summed_destroyed_stats->extent_stats[i].num_dirty += arena_stats->extent_stats[i].num_dirty;
            summed_destroyed_stats->extent_stats[i].num_muzzy += arena_stats->extent_stats[i].num_muzzy;
            summed_destroyed_stats->extent_stats[i].num_retained += arena_stats->extent_stats[i].num_retained;
            summed_destroyed_stats->extent_stats[i].dirty_bytes += arena_stats->extent_stats[i].dirty_bytes;
            summed_destroyed_stats->extent_stats[i].muzzy_bytes += arena_stats->extent_stats[i].muzzy_bytes;
            summed_destroyed_stats->extent_stats[i].retained_bytes += arena_stats->extent_stats[i].retained_bytes;
        }

        /// The HPA stats (`hpa_shard_stats_accum`) are always zero.
    }
}

}

/// jemalloc: ctl_arena_refresh
void mallctlArenaRefresh(
    ThreadState * thread_state, Arena * arena, MallctlArena * mallctl_summed_destroyed_arena, unsigned i, bool destroyed)
{
    MallctlArena * mallctl_arena = arenasI(i);

    mallctlArenaClear(mallctl_arena);
    mallctlArenaStatsArenaMerge(thread_state, mallctl_arena, arena);
    /// Merge into sum stats as well.
    mallctlArenaStatsSummedDestroyedMerge(mallctl_summed_destroyed_arena, mallctl_arena, destroyed);
}

/// jemalloc: ctl_arena_init
unsigned mallctlArenaInit(ThreadState & thread_state, const ArenaConfig * config)
{
    unsigned arena_idx;
    MallctlArena * mallctl_arena = mallctl_arenas->destroyed.last();
    if (mallctl_arena != nullptr)
    {
        mallctl_arenas->destroyed.remove(mallctl_arena);
        arena_idx = mallctl_arena->arena_idx;
    }
    else
    {
        arena_idx = mallctl_arenas->num_arenas;
    }

    /// Trigger stats allocation.
    if (arenasIImpl(&thread_state, arena_idx, false, true) == nullptr)
        return UINT_MAX;

    /// Initialize new arena.
    if (arenaInit(&thread_state, arena_idx, config) == nullptr)
        return UINT_MAX;

    if (arena_idx == mallctl_arenas->num_arenas)
        ++mallctl_arenas->num_arenas;

    return arena_idx;
}

/// jemalloc: ctl_refresh
void mallctlRefresh(ThreadState * thread_state)
{
    mallctl_mutex.assertOwner(thread_state);
    /// `mallctl_arenas->num_arenas` does not change underneath us since we hold `mallctl_mutex`.
    const unsigned num_arenas = mallctl_arenas->num_arenas;
    MallctlArena * mallctl_summed_arena = arenasI(MALLCTL_ARENAS_ALL);

    Arena * all_arenas[MALLOCX_ARENA_LIMIT];

    /// Clear sum stats, since they will be merged into by `ctl_arena_refresh`.
    mallctlArenaClear(mallctl_summed_arena);

    for (unsigned i = 0; i < num_arenas; ++i)
        all_arenas[i] = arenaGet(thread_state, i, false);

    for (unsigned i = 0; i < num_arenas; ++i)
    {
        MallctlArena * mallctl_arena = arenasI(i);
        bool initialized = (all_arenas[i] != nullptr);
        mallctl_arena->initialized = initialized;
        if (initialized)
            mallctlArenaRefresh(thread_state, all_arenas[i], mallctl_summed_arena, i, false);
    }

    if constexpr (config::stats)
    {
        MallctlArenaStats * summed_stats = mallctl_summed_arena->arena_stats;
        mallctl_stats->allocated = summed_stats->allocated_small + summed_stats->arena_stats.allocated_large;
        mallctl_stats->active = (mallctl_summed_arena->active_pages << LOG2_PAGE);
        mallctl_stats->metadata = summed_stats->arena_stats.base + summed_stats->arena_stats.internal.load(std::memory_order_relaxed);
        mallctl_stats->metadata_extent = summed_stats->arena_stats.metadata_extent;
        mallctl_stats->metadata_radix_tree = summed_stats->arena_stats.metadata_radix_tree;
        mallctl_stats->resident = summed_stats->arena_stats.resident;
        mallctl_stats->metadata_transparent_huge_pages = summed_stats->arena_stats.metadata_transparent_huge_pages;
        mallctl_stats->mapped = summed_stats->arena_stats.mapped;
        mallctl_stats->retained = summed_stats->arena_stats.page_allocator_shard_stats.page_allocator_stats.retained;

        mallctlBackgroundThreadStatsRead(thread_state);

        /// jemalloc: READ_GLOBAL_MUTEX_PROF_DATA
        auto read_global_mutex_profiling_data = [&](MutexProfilingGlobalIdx idx, Mutex & mutex)
        {
            MutexLock lock(thread_state, mutex);
            mutex.profilingRead(thread_state, mallctl_stats->mutex_profiling_data[idx]);
        };
        if (config::profiling && options.profiling)
        {
            read_global_mutex_profiling_data(global_profiling_mutex_profiling, backtrace_to_global_context_mutex);
            read_global_mutex_profiling_data(global_profiling_mutex_profiling_threads_data, all_thread_data_mutex);
            read_global_mutex_profiling_data(global_profiling_mutex_profiling_dump, profiling_dump_mutex);
            read_global_mutex_profiling_data(global_profiling_mutex_profiling_recent_alloc, profiling_recent_alloc_mutex);
            read_global_mutex_profiling_data(global_profiling_mutex_profiling_recent_dump, profiling_recent_dump_mutex);
            read_global_mutex_profiling_data(global_profiling_mutex_profiling_stats, profiling_stats_mutex);
        }
        if constexpr (config::background_thread)
        {
            MutexLock lock(thread_state, background_thread_lock);
            background_thread_lock.profilingRead(
                thread_state, mallctl_stats->mutex_profiling_data[global_profiling_mutex_background_thread]);
        }
        else
        {
            mallctl_stats->mutex_profiling_data[global_profiling_mutex_background_thread].reset();
        }

        /// We own the ctl mutex already.
        mallctl_mutex.profilingRead(thread_state, mallctl_stats->mutex_profiling_data[global_profiling_mutex_mallctl]);
    }
    ++mallctl_arenas->epoch;
}

namespace
{

/// Returns true on error (OOM). jemalloc: ctl_init
bool mallctlInit(ThreadState & thread_state)
{
    ThreadState * thread_state_ptr = &thread_state;
    MutexLock lock(thread_state_ptr, mallctl_mutex);
    if (mallctl_initialized.load(std::memory_order_relaxed))
        return false;

    /// Allocate demand-zeroed space for pointers to the full range of supported arena indices.
    if (mallctl_arenas == nullptr)
    {
        mallctl_arenas = static_cast<MallctlArenas *>(base0Get()->alloc(thread_state_ptr, sizeof(MallctlArenas), QUANTUM));
        if (mallctl_arenas == nullptr)
            return true;
    }

    if (config::stats && mallctl_stats == nullptr)
    {
        mallctl_stats = static_cast<MallctlStats *>(base0Get()->alloc(thread_state_ptr, sizeof(MallctlStats), QUANTUM));
        if (mallctl_stats == nullptr)
            return true;
    }

    /// Allocate space for the current full range of arenas here rather than doing it lazily elsewhere, in order to
    /// limit when OOM-caused errors can occur.
    MallctlArena * mallctl_summed_arena = arenasIImpl(thread_state_ptr, MALLCTL_ARENAS_ALL, false, true);
    if (mallctl_summed_arena == nullptr)
        return true;
    mallctl_summed_arena->initialized = true;

    MallctlArena * mallctl_destroyed_arena = arenasIImpl(thread_state_ptr, MALLCTL_ARENAS_DESTROYED, false, true);
    if (mallctl_destroyed_arena == nullptr)
        return true;
    mallctlArenaClear(mallctl_destroyed_arena);
    /// Don't toggle `mallctl_destroyed_arena` to initialized until an arena is actually destroyed, so that
    /// `arena.<i>.initialized` can be used to query whether the stats are relevant.

    mallctl_arenas->num_arenas = numArenasTotalGet();
    for (unsigned i = 0; i < mallctl_arenas->num_arenas; ++i)
    {
        if (arenasIImpl(thread_state_ptr, i, false, true) == nullptr)
            return true;
    }

    mallctl_arenas->destroyed.init();
    mallctlRefresh(thread_state_ptr);

    mallctl_initialized.store(true, std::memory_order_release);
    return false;
}

/// Returns true on error.
ALLOCATOR_ALWAYS_INLINE bool mallctlEnsureInitialized(ThreadState & thread_state)
{
    return !mallctl_initialized.load(std::memory_order_acquire) && mallctlInit(thread_state);
}

/// Equivalent to `strchrnul`.
const char * findDot(const char * element)
{
    const char * dot = std::strchr(element, '.');
    return dot != nullptr ? dot : element + std::strlen(element);
}

/// jemalloc: ctl_lookup
int mallctlLookup(
    ThreadState * thread_state,
    const MallctlNode * starting_node,
    const char * name,
    const MallctlNode ** ending_node_ptr,
    size_t * numeric_path_ptr,
    size_t * depth_ptr)
{
    const char * element = name;
    const char * dot = findDot(element);
    size_t element_length = static_cast<size_t>(dot - element);
    if (element_length == 0)
        return ENOENT;

    const MallctlNode * node = starting_node;
    for (size_t i = 0; i < *depth_ptr; ++i)
    {
        ALLOCATOR_ASSERT(node != nullptr);
        ALLOCATOR_ASSERT(node->num_children > 0);
        if (!node->isIndexed())
        {
            /// Children are named.
            const MallctlNode * parent_node = node;
            for (size_t j = 0; j < node->num_children; ++j)
            {
                const MallctlNode * child = &parent_node->children[j];
                if (std::strlen(child->name) == element_length && std::strncmp(element, child->name, element_length) == 0)
                {
                    node = child;
                    numeric_path_ptr[i] = j;
                    break;
                }
            }
            if (node == parent_node)
                return ENOENT;
        }
        else
        {
            /// Children are indexed.
            uintmax_t index = strToUIntMax(element, static_cast<const char **>(nullptr), 10);
            if (index == UINTMAX_MAX || index > SIZE_MAX)
                return ENOENT;

            if (!node->index(thread_state, numeric_path_ptr, *depth_ptr, static_cast<size_t>(index)))
                return ENOENT;
            node = node->children;
            numeric_path_ptr[i] = static_cast<size_t>(index);
        }

        /// Reached the end?
        if (node->isLeaf() || *dot == '\0')
        {
            /// Terminal node.
            if (*dot != '\0')
            {
                /// The name contains more elements than are in this path through the tree.
                return ENOENT;
            }
            /// Complete lookup successful.
            *depth_ptr = i + 1;
            break;
        }

        /// Update elm. An empty element is not rejected here: it matches no named child and has no digits.
        element = &dot[1];
        dot = findDot(element);
        element_length = static_cast<size_t>(dot - element);
    }
    if (ending_node_ptr != nullptr)
        *ending_node_ptr = node;
    return 0;
}

/// jemalloc: ctl_lookupbymib
int mallctlLookupByNumericPath(ThreadState * thread_state, const MallctlNode ** ending_node_ptr, const size_t * numeric_path, size_t numeric_path_length)
{
    const MallctlNode * node = mallctl_super_root_node;
    for (size_t i = 0; i < numeric_path_length; ++i)
    {
        ALLOCATOR_ASSERT(node != nullptr);
        /// jemalloc asserts `node->nchildren > 0` and walks past a terminal node in release builds (undefined
        /// behavior); a numeric path that is longer than the path is rejected instead.
        if (node->isLeaf())
            return ENOENT;
        if (!node->isIndexed())
        {
            /// Children are named.
            if (node->num_children <= numeric_path[i])
                return ENOENT;
            node = &node->children[numeric_path[i]];
        }
        else
        {
            /// Indexed element.
            if (!node->index(thread_state, numeric_path, numeric_path_length, numeric_path[i]))
                return ENOENT;
            node = node->children;
        }
    }
    ALLOCATOR_ASSERT(ending_node_ptr != nullptr);
    *ending_node_ptr = node;
    return 0;
}

}

/// --- Entry points ----------------------------------------------------------------------------------------------------

/// jemalloc: ctl_byname
int mallctlByName(
    ThreadState & thread_state, const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (mallctlEnsureInitialized(thread_state))
        return EAGAIN;

    size_t depth = MALLCTL_MAX_DEPTH;
    size_t numeric_path[MALLCTL_MAX_DEPTH];
    const MallctlNode * node = nullptr;
    int result = mallctlLookup(&thread_state, mallctl_super_root_node, name, &node, numeric_path, &depth);
    if (result != 0)
        return result;

    if (node != nullptr && node->isLeaf())
        return node->leaf(thread_state, numeric_path, depth, old_value, old_length_ptr, new_value, new_length);
    /// The name refers to a partial path through the ctl tree.
    return ENOENT;
}

/// jemalloc: ctl_nametomib
int mallctlNameToNumericPath(ThreadState & thread_state, const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr)
{
    if (mallctlEnsureInitialized(thread_state))
        return EAGAIN;
    return mallctlLookup(&thread_state, mallctl_super_root_node, name, nullptr, numeric_path_ptr, numeric_path_length_ptr);
}

/// jemalloc: ctl_bymib
int mallctlByNumericPath(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    if (mallctlEnsureInitialized(thread_state))
        return EAGAIN;

    const MallctlNode * node = nullptr;
    int result = mallctlLookupByNumericPath(&thread_state, &node, numeric_path, numeric_path_length);
    if (result != 0)
        return result;

    /// Call the ctl function.
    if (node != nullptr && node->isLeaf())
        return node->leaf(thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
    /// Partial numeric path.
    return ENOENT;
}

/// jemalloc: ctl_mibnametomib
int mallctlExtendNumericPathByName(ThreadState & thread_state, size_t * numeric_path, size_t numeric_path_length, const char * name, size_t * numeric_path_length_ptr)
{
    if (mallctlEnsureInitialized(thread_state))
        return EAGAIN;

    const MallctlNode * node = nullptr;
    int result = mallctlLookupByNumericPath(&thread_state, &node, numeric_path, numeric_path_length);
    if (result != 0)
        return result;
    if (node == nullptr || node->isLeaf())
        return ENOENT;

    ALLOCATOR_ASSERT(numeric_path_length_ptr != nullptr);
    ALLOCATOR_ASSERT(*numeric_path_length_ptr >= numeric_path_length);
    *numeric_path_length_ptr -= numeric_path_length;
    result = mallctlLookup(&thread_state, node, name, nullptr, numeric_path + numeric_path_length, numeric_path_length_ptr);
    *numeric_path_length_ptr += numeric_path_length;
    return result;
}

/// jemalloc: ctl_bymibname
int mallctlByNumericPathAndName(
    ThreadState & thread_state,
    size_t * numeric_path,
    size_t numeric_path_length,
    const char * name,
    size_t * numeric_path_length_ptr,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    if (mallctlEnsureInitialized(thread_state))
        return EAGAIN;

    const MallctlNode * node = nullptr;
    int result = mallctlLookupByNumericPath(&thread_state, &node, numeric_path, numeric_path_length);
    if (result != 0)
        return result;
    if (node == nullptr || node->isLeaf())
        return ENOENT;

    ALLOCATOR_ASSERT(numeric_path_length_ptr != nullptr);
    ALLOCATOR_ASSERT(*numeric_path_length_ptr >= numeric_path_length);
    *numeric_path_length_ptr -= numeric_path_length;
    /// The same node supplies the starting node and stores the ending node.
    result = mallctlLookup(&thread_state, node, name, &node, numeric_path + numeric_path_length, numeric_path_length_ptr);
    *numeric_path_length_ptr += numeric_path_length;
    if (result != 0)
        return result;

    if (node != nullptr && node->isLeaf())
        return node->leaf(thread_state, numeric_path, *numeric_path_length_ptr, old_value, old_length_ptr, new_value, new_length);
    /// The name refers to a partial path through the ctl tree.
    return ENOENT;
}

/// jemalloc: ctl_boot
bool mallctlBoot()
{
    if (mallctl_mutex.init("ctl", MutexRank::MALLCTL, MutexLockOrder::RankExclusive))
        return true;
    mallctl_initialized.store(false, std::memory_order_relaxed);
    return false;
}

/// jemalloc: ctl_prefork
void mallctlPrefork(ThreadState * thread_state)
{
    mallctl_mutex.prefork(thread_state);
}

/// jemalloc: ctl_postfork_parent
void mallctlPostforkParent(ThreadState * thread_state)
{
    mallctl_mutex.postforkParent(thread_state);
}

/// jemalloc: ctl_postfork_child
void mallctlPostforkChild(ThreadState * thread_state)
{
    mallctl_mutex.postforkChild(thread_state);
}

/// jemalloc: ctl_mtx_assert_held
void mallctlMutexAssertHeld(ThreadState * thread_state)
{
    mallctl_mutex.assertOwner(thread_state);
}

/// --- Leaves and index functions that depend only on the ctl state ---------------------------------------------------

namespace mallctl
{

/// jemalloc: version_ctl (CTL_RO_NL_GEN)
int version(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyNoLock<const char *, [] { return JEMALLOC_VERSION; }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// Any write refreshes the statistics snapshot (the written value is ignored); reads return the current epoch.
/// jemalloc: epoch_ctl
int epoch(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    uint64_t new_setting;
    if (int result = write(new_value, new_length, new_setting))
        return result;
    if (new_value != nullptr)
        mallctlRefresh(&thread_state);
    return read(old_value, old_length_ptr, mallctl_arenas->epoch);
}

/// Accepts `MALLCTL_ARENAS_ALL`, `MALLCTL_ARENAS_DESTROYED` and every `i <= num_arenas` (`i == num_arenas` is the
/// deprecated alias of `MALLCTL_ARENAS_ALL`).
/// jemalloc: arena_i_index
bool arenaIIndex(ThreadState * thread_state, const size_t *, size_t, size_t i)
{
    MutexLock lock(thread_state, mallctl_mutex);
    switch (i)
    {
        case MALLCTL_ARENAS_ALL:
        case MALLCTL_ARENAS_DESTROYED: return true;
        default: return i <= mallctl_arenas->num_arenas;
    }
}

/// jemalloc: stats_arenas_i_index
bool statsArenasIIndex(ThreadState * thread_state, const size_t *, size_t, size_t i)
{
    MutexLock lock(thread_state, mallctl_mutex);
    return !mallctlArenasIVerify(i);
}

/// jemalloc: experimental_arenas_i_index
bool experimentalArenasIIndex(ThreadState * thread_state, const size_t *, size_t, size_t i)
{
    MutexLock lock(thread_state, mallctl_mutex);
    return !mallctlArenasIVerify(i);
}

}

}
