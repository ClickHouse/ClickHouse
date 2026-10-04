/// `experimental.*` except the profiler's leaves (jemalloc: `ctl.c`), and the utilization queries (jemalloc:
/// `inspect.c`). The index function of `experimental.arenas` is in Ctl.cpp.

#include <allocator/CtlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Imalloc.h>
#include <allocator/Sanitizer.h>
#include <allocator/ThreadState.h>

namespace jemalloc
{

namespace
{

/// --- inspect.c -----------------------------------------------------------------------------------------------------

/// jemalloc: inspect_extent_util_stats_t
struct InspectExtentUtilStats
{
    size_t nfree;
    size_t nregs;
    size_t size;
};

static_assert(sizeof(InspectExtentUtilStats) == sizeof(size_t) * 3);

/// jemalloc: inspect_extent_util_stats_verbose_t
struct InspectExtentUtilStatsVerbose
{
    void * slabcur_addr;
    size_t nfree;
    size_t nregs;
    size_t size;
    size_t bin_nfree;
    size_t bin_nregs;
};

static_assert(sizeof(InspectExtentUtilStatsVerbose) == sizeof(void *) + sizeof(size_t) * 5);

/// jemalloc: inspect_extent_util_stats_get
void inspectExtentUtilStatsGet(ThreadState * tsdn, const void * ptr, size_t * nfree, size_t * nregs, size_t * size)
{
    JE_ASSERT(ptr != nullptr && nfree != nullptr && nregs != nullptr && size != nullptr);

    const Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    if (JE_UNLIKELY(edata == nullptr))
    {
        *nfree = *nregs = *size = 0;
        return;
    }

    *size = edata->size();
    if (!edata->slab())
    {
        *nfree = 0;
        *nregs = 1;
    }
    else
    {
        *nfree = edata->nfree();
        *nregs = bin_infos[edata->szind()].nregs;
        JE_ASSERT(*nfree <= *nregs);
        JE_ASSERT(*nfree * edata->usize() <= *size);
    }
}

/// jemalloc: inspect_extent_util_stats_verbose_get
void inspectExtentUtilStatsVerboseGet(
    ThreadState * tsdn,
    const void * ptr,
    size_t * nfree,
    size_t * nregs,
    size_t * size,
    size_t * bin_nfree,
    size_t * bin_nregs,
    void ** slabcur_addr)
{
    JE_ASSERT(
        ptr != nullptr && nfree != nullptr && nregs != nullptr && size != nullptr && bin_nfree != nullptr
        && bin_nregs != nullptr && slabcur_addr != nullptr);

    const Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    if (JE_UNLIKELY(edata == nullptr))
    {
        *nfree = *nregs = *size = *bin_nfree = *bin_nregs = 0;
        *slabcur_addr = nullptr;
        return;
    }

    *size = edata->size();
    if (!edata->slab())
    {
        *nfree = *bin_nfree = *bin_nregs = 0;
        *nregs = 1;
        *slabcur_addr = nullptr;
        return;
    }

    *nfree = edata->nfree();
    const szind_t szind = edata->szind();
    *nregs = bin_infos[szind].nregs;
    JE_ASSERT(*nfree <= *nregs);
    JE_ASSERT(*nfree * edata->usize() <= *size);

    Arena * arena = arenas[edata->arenaInd()].load(std::memory_order_relaxed);
    JE_ASSERT(arena != nullptr);
    const unsigned binshard = edata->binshard();
    Bin * bin = arenaGetBin(arena, szind, binshard);

    MutexLock lock(tsdn, bin->lock);
    if constexpr (config::stats)
    {
        *bin_nregs = *nregs * bin->stats.curslabs;
        JE_ASSERT(*bin_nregs >= bin->stats.curregs);
        *bin_nfree = *bin_nregs - bin->stats.curregs;
    }
    else
    {
        *bin_nfree = *bin_nregs = 0;
    }
    Extent * slab;
    if (bin->slabcur != nullptr)
        slab = bin->slabcur;
    else
        slab = bin->slabs_nonfull.first();
    *slabcur_addr = slab != nullptr ? slab->addr() : nullptr;
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

namespace ctl
{

/// `hook.c` is dropped. jemalloc: experimental_hooks_install_ctl, experimental_hooks_remove_ctl
JE_CTL_DROPPED(experimentalHooksInstall)
JE_CTL_DROPPED(experimentalHooksRemove)

/// For integration test purpose only. No plan to move out of experimental.
/// jemalloc: experimental_hooks_safety_check_abort_ctl
int experimentalHooksSafetyCheckAbort(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    if (newp != nullptr)
    {
        if (newlen != sizeof(SafetyCheckAbortHook))
            return EINVAL;
        SafetyCheckAbortHook hook = nullptr;
        if (int ret = write(newp, newlen, hook))
            return ret;
        safetyCheckSetAbort(hook);
    }
    return 0;
}

/// User thread event hooks are dropped. jemalloc: experimental_hooks_thread_event_ctl
JE_CTL_DROPPED(experimentalHooksThreadEvent)

/// jemalloc: experimental_thread_activity_callback_ctl
int experimentalThreadActivityCallback(
    ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::stats)
        return ENOENT;

    ActivityCallbackThunk t_old = tsd.activity_callback_thunk;
    if (int ret = read(oldp, oldlenp, t_old))
        return ret;

    if (newp != nullptr)
    {
        ActivityCallbackThunk t_new = {nullptr, nullptr};
        if (int ret = write(newp, newlen, t_new))
            return ret;
        tsd.activity_callback_thunk = t_new;
    }
    return 0;
}

/// Outputs six memory utilization entries for an input pointer (see the comment in jemalloc's `ctl.c`): the address
/// of the extent a potential reallocation would go into, and the number of free regions, the number of regions and
/// the size of the extent the pointer resides in, and the number of free regions and of regions in its bin. Returns
/// `EINVAL` without touching anything unless `*oldlenp == sizeof(void *) + sizeof(size_t) * 5`. If no extent is found
/// for the pointer, all output fields are zeroed.
/// jemalloc: experimental_utilization_query_ctl
int experimentalUtilizationQuery(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (oldp == nullptr || oldlenp == nullptr || *oldlenp != sizeof(InspectExtentUtilStatsVerbose) || newp == nullptr)
        return EINVAL;

    void * ptr = nullptr;
    if (int ret = write(newp, newlen, ptr))
        return ret;
    auto * util_stats = static_cast<InspectExtentUtilStatsVerbose *>(oldp);
    inspectExtentUtilStatsVerboseGet(
        &tsd,
        ptr,
        &util_stats->nfree,
        &util_stats->nregs,
        &util_stats->size,
        &util_stats->bin_nfree,
        &util_stats->bin_nregs,
        &util_stats->slabcur_addr);
    return 0;
}

/// Given an input array of pointers (`newp`, `newlen`), outputs three entries of type `size_t` for each pointer
/// about the extent it resides in: the number of free regions, the number of regions, and the size (see the comment
/// in jemalloc's `ctl.c`). Returns `EINVAL` without touching anything unless `newlen == n * sizeof(void *)`,
/// `*oldlenp == n * sizeof(size_t) * 3`, `n > 0`. Pointers without an extent get zeros.
/// jemalloc: experimental_utilization_batch_query_ctl
int experimentalUtilizationBatchQuery(
    ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    const size_t len = newlen / sizeof(const void *);
    if (oldp == nullptr || oldlenp == nullptr || newp == nullptr || newlen == 0 || newlen != len * sizeof(const void *)
        || *oldlenp != len * sizeof(InspectExtentUtilStats))
        return EINVAL;

    void ** ptrs = static_cast<void **>(newp);
    auto * util_stats = static_cast<InspectExtentUtilStats *>(oldp);
    for (size_t i = 0; i < len; ++i)
        inspectExtentUtilStatsGet(&tsd, ptrs[i], &util_stats[i].nfree, &util_stats[i].nregs, &util_stats[i].size);
    return 0;
}

/// Exposes the underlying counter of active pages for fast reads.
/// jemalloc: experimental_arenas_i_pactivep_ctl
int experimentalArenasIPactivep(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (oldp == nullptr || oldlenp == nullptr || *oldlenp != sizeof(size_t *))
        return EINVAL;

    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = readOnly(newp, newlen))
        return ret;
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 2, arena_ind))
        return ret;
    Arena * arena;
    if (arena_ind < narenasTotalGet() && (arena = arenaGet(&tsd, arena_ind, false)) != nullptr)
    {
        static_assert(sizeof(std::atomic<size_t>) == sizeof(size_t));
        size_t * pactivep = reinterpret_cast<size_t *>(&arena->pa_shard.nactive);
        return read(oldp, oldlenp, pactivep);
    }
    return EFAULT;
}

/// Custom extent hooks are not supported: like `arenas.create`, only the default table is accepted (`EINVAL`
/// otherwise).
/// jemalloc: experimental_arenas_create_ext_ctl
int experimentalArenasCreateExt(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);

    ArenaConfig config = arena_config_default;
    if (int ret = verifyRead<unsigned>(oldp, oldlenp))
        return ret;
    if (int ret = write(newp, newlen, config))
        return ret;
    if (config.extent_hooks != &ehooks_default_extent_hooks)
        return EINVAL;

    unsigned arena_ind = ctlArenaInit(tsd, &config);
    if (arena_ind == UINT_MAX)
        return EAGAIN;
    return read(oldp, oldlenp, arena_ind);
}

/// jemalloc: experimental_batch_alloc_ctl
int experimentalBatchAlloc(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = verifyRead<size_t>(oldp, oldlenp))
        return ret;

    BatchAllocPacket batch_alloc_packet;
    if (int ret = assuredWrite(newp, newlen, batch_alloc_packet))
        return ret;
    size_t filled = batchAlloc(batch_alloc_packet.ptrs, batch_alloc_packet.num, batch_alloc_packet.size, batch_alloc_packet.flags);
    return read(oldp, oldlenp, filled);
}

}

}
