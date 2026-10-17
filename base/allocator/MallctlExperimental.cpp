/// `experimental.*` except the profiler's leaves (jemalloc: `ctl.c`), and the utilization queries (jemalloc:
/// `inspect.c`). The index function of `experimental.arenas` is in Mallctl.cpp.

#include <allocator/MallctlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/InternalMalloc.h>
#include <allocator/Sanitizer.h>
#include <allocator/ThreadState.h>

namespace jemalloc
{

namespace
{

/// --- inspect.c -----------------------------------------------------------------------------------------------------

/// jemalloc: inspect_extent_util_stats_t
struct InspectExtentUtilizationStats
{
    size_t num_free;
    size_t num_regions;
    size_t size;
};

static_assert(sizeof(InspectExtentUtilizationStats) == sizeof(size_t) * 3);

/// jemalloc: inspect_extent_util_stats_verbose_t
struct InspectExtentUtilizationStatsVerbose
{
    void * current_slab_addr;
    size_t num_free;
    size_t num_regions;
    size_t size;
    size_t bin_num_free;
    size_t bin_num_regions;
};

static_assert(sizeof(InspectExtentUtilizationStatsVerbose) == sizeof(void *) + sizeof(size_t) * 5);

/// jemalloc: inspect_extent_util_stats_get
void inspectExtentUtilizationStatsGet(ThreadState * thread_state, const void * ptr, size_t * num_free, size_t * num_regions, size_t * size)
{
    ALLOCATOR_ASSERT(ptr != nullptr && num_free != nullptr && num_regions != nullptr && size != nullptr);

    const Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    if (ALLOCATOR_UNLIKELY(extent == nullptr))
    {
        *num_free = *num_regions = *size = 0;
        return;
    }

    *size = extent->size();
    if (!extent->slab())
    {
        *num_free = 0;
        *num_regions = 1;
    }
    else
    {
        *num_free = extent->numFree();
        *num_regions = bin_infos[extent->sizeClassIdx()].num_regions;
        ALLOCATOR_ASSERT(*num_free <= *num_regions);
        ALLOCATOR_ASSERT(*num_free * extent->usableSize() <= *size);
    }
}

/// jemalloc: inspect_extent_util_stats_verbose_get
void inspectExtentUtilizationStatsVerboseGet(
    ThreadState * thread_state,
    const void * ptr,
    size_t * num_free,
    size_t * num_regions,
    size_t * size,
    size_t * bin_num_free,
    size_t * bin_num_regions,
    void ** current_slab_addr)
{
    ALLOCATOR_ASSERT(
        ptr != nullptr && num_free != nullptr && num_regions != nullptr && size != nullptr && bin_num_free != nullptr
        && bin_num_regions != nullptr && current_slab_addr != nullptr);

    const Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    if (ALLOCATOR_UNLIKELY(extent == nullptr))
    {
        *num_free = *num_regions = *size = *bin_num_free = *bin_num_regions = 0;
        *current_slab_addr = nullptr;
        return;
    }

    *size = extent->size();
    if (!extent->slab())
    {
        *num_free = *bin_num_free = *bin_num_regions = 0;
        *num_regions = 1;
        *current_slab_addr = nullptr;
        return;
    }

    *num_free = extent->numFree();
    const SizeClassIdx size_class_idx = extent->sizeClassIdx();
    *num_regions = bin_infos[size_class_idx].num_regions;
    ALLOCATOR_ASSERT(*num_free <= *num_regions);
    ALLOCATOR_ASSERT(*num_free * extent->usableSize() <= *size);

    Arena * arena = arenas[extent->arenaIdx()].load(std::memory_order_relaxed);
    ALLOCATOR_ASSERT(arena != nullptr);
    const unsigned bin_shard = extent->binShard();
    Bin * bin = arenaGetBin(arena, size_class_idx, bin_shard);

    MutexLock lock(thread_state, bin->lock);
    if constexpr (config::stats)
    {
        *bin_num_regions = *num_regions * bin->stats.current_slabs;
        ALLOCATOR_ASSERT(*bin_num_regions >= bin->stats.current_regions);
        *bin_num_free = *bin_num_regions - bin->stats.current_regions;
    }
    else
    {
        *bin_num_free = *bin_num_regions = 0;
    }
    Extent * slab;
    if (bin->current_slab != nullptr)
        slab = bin->current_slab;
    else
        slab = bin->slabs_non_full.first();
    *current_slab_addr = slab != nullptr ? slab->addr() : nullptr;
}

/// jemalloc: batch_alloc_packet_t
struct BatchAllocPacket
{
    void ** ptrs;
    size_t num;
    size_t size;
    int flags;
};

}

namespace mallctl
{

/// `hook.c` is dropped. jemalloc: experimental_hooks_install_ctl, experimental_hooks_remove_ctl
ALLOCATOR_MALLCTL_DROPPED(experimentalHooksInstall)
ALLOCATOR_MALLCTL_DROPPED(experimentalHooksRemove)

/// For integration test purpose only. No plan to move out of experimental.
/// jemalloc: experimental_hooks_safety_check_abort_ctl
int experimentalHooksSafetyCheckAbort(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    if (new_value != nullptr)
    {
        if (new_length != sizeof(SafetyCheckAbortHook))
            return EINVAL;
        SafetyCheckAbortHook hook = nullptr;
        if (int result = write(new_value, new_length, hook))
            return result;
        safetyCheckSetAbort(hook);
    }
    return 0;
}

/// User thread event hooks are dropped. jemalloc: experimental_hooks_thread_event_ctl
ALLOCATOR_MALLCTL_DROPPED(experimentalHooksThreadEvent)

/// jemalloc: experimental_thread_activity_callback_ctl
int experimentalThreadActivityCallback(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::stats)
        return ENOENT;

    ActivityCallbackThunk t_old = thread_state.activity_callback_thunk;
    if (int result = read(old_value, old_length_ptr, t_old))
        return result;

    if (new_value != nullptr)
    {
        ActivityCallbackThunk t_new = {nullptr, nullptr};
        if (int result = write(new_value, new_length, t_new))
            return result;
        thread_state.activity_callback_thunk = t_new;
    }
    return 0;
}

/// Outputs six memory utilization entries for an input pointer (see the comment in jemalloc's `ctl.c`): the address
/// of the extent a potential reallocation would go into, and the number of free regions, the number of regions and
/// the size of the extent the pointer resides in, and the number of free regions and of regions in its bin. Returns
/// `EINVAL` without touching anything unless `*old_length_ptr == sizeof(void *) + sizeof(size_t) * 5`. If no extent is found
/// for the pointer, all output fields are zeroed.
/// jemalloc: experimental_utilization_query_ctl
int experimentalUtilizationQuery(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (old_value == nullptr || old_length_ptr == nullptr || *old_length_ptr != sizeof(InspectExtentUtilizationStatsVerbose)
        || new_value == nullptr)
        return EINVAL;

    void * ptr = nullptr;
    if (int result = write(new_value, new_length, ptr))
        return result;
    auto * utilization_stats = static_cast<InspectExtentUtilizationStatsVerbose *>(old_value);
    inspectExtentUtilizationStatsVerboseGet(
        &thread_state,
        ptr,
        &utilization_stats->num_free,
        &utilization_stats->num_regions,
        &utilization_stats->size,
        &utilization_stats->bin_num_free,
        &utilization_stats->bin_num_regions,
        &utilization_stats->current_slab_addr);
    return 0;
}

/// Given an input array of pointers (`new_value`, `new_length`), outputs three entries of type `size_t` for each pointer
/// about the extent it resides in: the number of free regions, the number of regions, and the size (see the comment
/// in jemalloc's `ctl.c`). Returns `EINVAL` without touching anything unless `new_length == n * sizeof(void *)`,
/// `*old_length_ptr == n * sizeof(size_t) * 3`, `n > 0`. Pointers without an extent get zeros.
/// jemalloc: experimental_utilization_batch_query_ctl
int experimentalUtilizationBatchQuery(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    const size_t len = new_length / sizeof(const void *);
    if (old_value == nullptr || old_length_ptr == nullptr || new_value == nullptr || new_length == 0
        || new_length != len * sizeof(const void *) || *old_length_ptr != len * sizeof(InspectExtentUtilizationStats))
        return EINVAL;

    void ** ptrs = static_cast<void **>(new_value);
    auto * utilization_stats = static_cast<InspectExtentUtilizationStats *>(old_value);
    for (size_t i = 0; i < len; ++i)
        inspectExtentUtilizationStatsGet(
            &thread_state, ptrs[i], &utilization_stats[i].num_free, &utilization_stats[i].num_regions, &utilization_stats[i].size);
    return 0;
}

/// Exposes the underlying counter of active pages for fast reads.
/// jemalloc: experimental_arenas_i_pactivep_ctl
int experimentalArenasIActivePagesPtr(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (old_value == nullptr || old_length_ptr == nullptr || *old_length_ptr != sizeof(size_t *))
        return EINVAL;

    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = readOnly(new_value, new_length))
        return result;
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 2, arena_idx))
        return result;
    Arena * arena;
    if (arena_idx < numArenasTotalGet() && (arena = arenaGet(&thread_state, arena_idx, false)) != nullptr)
    {
        static_assert(sizeof(std::atomic<size_t>) == sizeof(size_t));
        size_t * active_pages_ptr = reinterpret_cast<size_t *>(&arena->page_allocator_shard.num_active);
        return read(old_value, old_length_ptr, active_pages_ptr);
    }
    return EFAULT;
}

/// Custom extent hooks are not supported: like `arenas.create`, only the default table is accepted (`EINVAL`
/// otherwise).
/// jemalloc: experimental_arenas_create_ext_ctl
int experimentalArenasCreateExtended(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);

    ArenaConfig config = arena_config_default;
    if (int result = verifyRead<unsigned>(old_value, old_length_ptr))
        return result;
    if (int result = write(new_value, new_length, config))
        return result;
    if (config.extent_hooks_ptr != &extent_hooks_default_extent_hooks)
        return EINVAL;

    unsigned arena_idx = mallctlArenaInit(thread_state, &config);
    if (arena_idx == UINT_MAX)
        return EAGAIN;
    return read(old_value, old_length_ptr, arena_idx);
}

/// jemalloc: experimental_batch_alloc_ctl
int experimentalBatchAlloc(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = verifyRead<size_t>(old_value, old_length_ptr))
        return result;

    BatchAllocPacket batch_alloc_packet;
    if (int result = assuredWrite(new_value, new_length, batch_alloc_packet))
        return result;
    size_t filled = batchAlloc(batch_alloc_packet.ptrs, batch_alloc_packet.num, batch_alloc_packet.size, batch_alloc_packet.flags);
    return read(old_value, old_length_ptr, filled);
}

}

}
