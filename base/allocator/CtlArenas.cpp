/// `arena.<i>.*` and `arenas.*` (jemalloc: `ctl.c`). The constant `arenas.*` leaves and the index functions of
/// `arenas.bin` and `arenas.lextent` are generated in CtlTree.cpp; the index function of `arena` is in Ctl.cpp.

#include <allocator/CtlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc::ctl
{

/// The value reflects the last `epoch` refresh (except `arena.<i>.destroy`, which updates it immediately).
/// jemalloc: arena_i_initialized_ctl
int arenaIInitialized(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;

    bool initialized;
    {
        MutexLock lock(&tsd, ctl_mtx);
        initialized = arenasI(arena_ind)->initialized;
    }
    return read(oldp, oldlenp, initialized);
}

namespace
{

/// `arena_ind` is the index of `arena.<i>`: `MALLCTL_ARENAS_ALL` (or the deprecated alias `narenas`) decays every
/// arena.
/// jemalloc: arena_i_decay
void arenaIDecayImpl(ThreadState * tsdn, unsigned arena_ind, bool all)
{
    ctl_mtx.lock(tsdn);
    unsigned narenas = ctl_arenas->narenas;

    /// Access via index narenas is deprecated, and scheduled for removal in 6.0.0.
    if (arena_ind == MALLCTL_ARENAS_ALL || arena_ind == narenas)
    {
        Arena * tarenas[MALLOCX_ARENA_LIMIT];
        for (unsigned i = 0; i < narenas; ++i)
            tarenas[i] = arenaGet(tsdn, i, false);

        /// No further need to hold ctl_mtx, since narenas and tarenas contain everything needed below.
        ctl_mtx.unlock(tsdn);

        for (unsigned i = 0; i < narenas; ++i)
        {
            if (tarenas[i] != nullptr)
                arenaDecay(tsdn, tarenas[i], false, all);
        }
    }
    else
    {
        /// jemalloc reads `arenas[4097]` for `MALLCTL_ARENAS_DESTROYED` (out of bounds, undefined behavior); here it
        /// is a no-op.
        Arena * tarena = (arena_ind < narenas) ? arenaGet(tsdn, arena_ind, false) : nullptr;

        /// No further need to hold ctl_mtx.
        ctl_mtx.unlock(tsdn);

        if (tarena != nullptr)
            arenaDecay(tsdn, tarena, false, all);
    }
}

/// jemalloc: arena_i_reset_destroy_helper
int arenaIResetDestroyHelper(
    ThreadState & tsd,
    const size_t * mib,
    void * oldp,
    size_t * oldlenp,
    void * newp,
    size_t newlen,
    unsigned & arena_ind,
    Arena *& arena)
{
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;

    /// jemalloc reads `arenas[4096]` / `arenas[4097]` (out of bounds) for the merged slots; here they do not exist.
    arena = arena_ind < MALLOCX_ARENA_LIMIT ? arenaGet(&tsd, arena_ind, false) : nullptr;
    if (arena == nullptr || arenaIsAuto(arena))
        return EFAULT;
    return 0;
}

/// Temporarily disable the background thread during arena reset (`background_thread_lock` stays locked).
/// jemalloc: arena_reset_prepare_background_thread
void arenaResetPrepareBackgroundThread(ThreadState & tsd, unsigned arena_ind)
{
    if constexpr (config::background_thread)
    {
        background_thread_lock.lock(&tsd);
        if (backgroundThreadEnabled())
        {
            BackgroundThreadInfo * info = backgroundThreadInfoGet(arena_ind);
            JE_ASSERT(info->state == BackgroundThreadState::Started);
            MutexLock lock(&tsd, info->mtx);
            info->state = BackgroundThreadState::Paused;
        }
    }
}

/// jemalloc: arena_reset_finish_background_thread
void arenaResetFinishBackgroundThread(ThreadState & tsd, unsigned arena_ind)
{
    if constexpr (config::background_thread)
    {
        if (backgroundThreadEnabled())
        {
            BackgroundThreadInfo * info = backgroundThreadInfoGet(arena_ind);
            JE_ASSERT(info->state == BackgroundThreadState::Paused);
            MutexLock lock(&tsd, info->mtx);
            info->state = BackgroundThreadState::Started;
        }
        background_thread_lock.unlock(&tsd);
    }
}

/// jemalloc: arena_i_decay_ms_ctl_impl
int arenaIDecayMsImpl(
    ThreadState & tsd, const size_t * mib, void * oldp, size_t * oldlenp, void * newp, size_t newlen, bool dirty)
{
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    /// jemalloc reads out of bounds for the merged slots (4096, 4097); here they do not exist.
    Arena * arena = arena_ind < MALLOCX_ARENA_LIMIT ? arenaGet(&tsd, arena_ind, false) : nullptr;
    if (arena == nullptr)
        return EFAULT;
    ExtentState state = dirty ? extent_state_dirty : extent_state_muzzy;

    if (oldp != nullptr && oldlenp != nullptr)
    {
        ssize_t oldval = arenaDecayMsGet(arena, state);
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;
    }
    if (newp != nullptr)
    {
        if (newlen != sizeof(ssize_t))
            return EINVAL;
        if (arenaDecayMsSet(&tsd, arena, state, *static_cast<const ssize_t *>(newp)))
            return EFAULT;
    }
    return 0;
}

/// jemalloc: arenas_decay_ms_ctl_impl
int arenasDecayMsImpl(void * oldp, size_t * oldlenp, void * newp, size_t newlen, bool dirty)
{
    if (oldp != nullptr && oldlenp != nullptr)
    {
        ssize_t oldval = dirty ? arenaDirtyDecayMsDefaultGet() : arenaMuzzyDecayMsDefaultGet();
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;
    }
    if (newp != nullptr)
    {
        if (newlen != sizeof(ssize_t))
            return EINVAL;
        ssize_t newval = *static_cast<const ssize_t *>(newp);
        if (dirty ? arenaDirtyDecayMsDefaultSet(newval) : arenaMuzzyDecayMsDefaultSet(newval))
            return EFAULT;
    }
    return 0;
}

}

/// jemalloc: arena_i_decay_ctl
int arenaIDecay(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    arenaIDecayImpl(&tsd, arena_ind, false);
    return 0;
}

/// jemalloc: arena_i_purge_ctl
int arenaIPurge(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    arenaIDecayImpl(&tsd, arena_ind, true);
    return 0;
}

/// Only manual arenas can be reset.
/// jemalloc: arena_i_reset_ctl
int arenaIReset(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    unsigned arena_ind;
    Arena * arena;
    if (int ret = arenaIResetDestroyHelper(tsd, mib, oldp, oldlenp, newp, newlen, arena_ind, arena))
        return ret;

    arenaResetPrepareBackgroundThread(tsd, arena_ind);
    arenaReset(tsd, arena);
    arenaResetFinishBackgroundThread(tsd, arena_ind);
    return 0;
}

/// Only manual arenas without threads can be destroyed. The stats are merged into the `MALLCTL_ARENAS_DESTROYED`
/// slot and the index is recycled by `arenas.create`.
/// jemalloc: arena_i_destroy_ctl
int arenaIDestroy(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);

    unsigned arena_ind;
    Arena * arena;
    if (int ret = arenaIResetDestroyHelper(tsd, mib, oldp, oldlenp, newp, newlen, arena_ind, arena))
        return ret;

    if (arenaNthreadsGet(arena, false) != 0 || arenaNthreadsGet(arena, true) != 0)
        return EFAULT;

    arenaResetPrepareBackgroundThread(tsd, arena_ind);
    /// Merge stats after resetting and purging arena.
    arenaReset(tsd, arena);
    arenaDecay(&tsd, arena, false, true);
    CtlArena * ctl_darena = arenasI(MALLCTL_ARENAS_DESTROYED);
    ctl_darena->initialized = true;
    ctlArenaRefresh(&tsd, arena, ctl_darena, arena_ind, true);
    /// Destroy arena.
    arenaDestroy(tsd, arena);
    CtlArena * ctl_arena = arenasI(arena_ind);
    ctl_arena->initialized = false;
    /// Record arena index for later recycling via arenas.create.
    decltype(ctl_arenas->destroyed)::elementInit(ctl_arena);
    ctl_arenas->destroyed.tailInsert(ctl_arena);
    arenaResetFinishBackgroundThread(tsd, arena_ind);
    return 0;
}

/// DSS is dropped, but the precedence settings are stored and reported. Note that the returned "old" value is read
/// after the set (so it is the new setting).
/// jemalloc: arena_i_dss_ctl
int arenaIDss(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    const char * dss = nullptr;
    if (int ret = write(newp, newlen, dss))
        return ret;
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;

    DssPrec dss_prec = DssPrec::Limit;
    if (dss != nullptr)
    {
        bool match = false;
        for (unsigned i = 0; i < unsigned(DssPrec::Limit); ++i)
        {
            if (std::strcmp(dss_prec_names[i], dss) == 0)
            {
                dss_prec = DssPrec(i);
                match = true;
                break;
            }
        }
        if (!match)
            return EINVAL;
    }

    /// Access via index narenas is deprecated, and scheduled for removal in 6.0.0.
    DssPrec dss_prec_old;
    if (arena_ind == MALLCTL_ARENAS_ALL || arena_ind == ctl_arenas->narenas)
    {
        if (dss_prec != DssPrec::Limit && extentDssPrecSet(dss_prec))
            return EFAULT;
        dss_prec_old = extentDssPrecGet();
    }
    else
    {
        /// jemalloc reads `arenas[4097]` (out of bounds) for `MALLCTL_ARENAS_DESTROYED`; here it does not exist.
        Arena * arena = arena_ind < MALLOCX_ARENA_LIMIT ? arenaGet(&tsd, arena_ind, false) : nullptr;
        if (arena == nullptr || (dss_prec != DssPrec::Limit && arenaDssPrecSet(arena, dss_prec)))
            return EFAULT;
        dss_prec_old = arenaDssPrecGet(arena);
    }

    dss = dss_prec_names[unsigned(dss_prec_old)];
    return read(oldp, oldlenp, dss);
}

/// No validation of the value.
/// jemalloc: arena_i_oversize_threshold_ctl
int arenaIOversizeThreshold(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;

    /// jemalloc reads out of bounds for the merged slots (4096, 4097); here they do not exist.
    Arena * arena = arena_ind < MALLOCX_ARENA_LIMIT ? arenaGet(&tsd, arena_ind, false) : nullptr;
    if (arena == nullptr)
        return EFAULT;

    if (oldp != nullptr && oldlenp != nullptr)
    {
        size_t oldval = arena->pa_shard.pac.oversize_threshold.load(std::memory_order_relaxed);
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;
    }
    if (newp != nullptr)
    {
        if (newlen != sizeof(size_t))
            return EINVAL;
        arena->pa_shard.pac.oversize_threshold.store(*static_cast<const size_t *>(newp), std::memory_order_relaxed);
    }
    return 0;
}

/// jemalloc: arena_i_dirty_decay_ms_ctl
int arenaIDirtyDecayMs(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return arenaIDecayMsImpl(tsd, mib, oldp, oldlenp, newp, newlen, true);
}

/// jemalloc: arena_i_muzzy_decay_ms_ctl
int arenaIMuzzyDecayMs(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return arenaIDecayMsImpl(tsd, mib, oldp, oldlenp, newp, newlen, false);
}

/// Custom extent hooks are not supported (the default hooks are always used): writing any other table than
/// `ehooks_default_extent_hooks` returns `EINVAL` (jemalloc would install it). Otherwise as in jemalloc: writing to
/// a missing auto arena creates it.
/// jemalloc: arena_i_extent_hooks_ctl
int arenaIExtentHooks(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    if (arena_ind >= narenasTotalGet())
        return EFAULT;

    Arena * arena = arenaGet(&tsd, arena_ind, false);
    extent_hooks_t * old_extent_hooks;
    if (arena == nullptr)
    {
        if (arena_ind >= narenas_auto)
            return EFAULT;
        old_extent_hooks = const_cast<extent_hooks_t *>(&ehooks_default_extent_hooks);
        if (int ret = read(oldp, oldlenp, old_extent_hooks))
            return ret;
        if (newp != nullptr)
        {
            /// Initialize a new arena as a side effect.
            extent_hooks_t * new_extent_hooks = nullptr;
            if (int ret = write(newp, newlen, new_extent_hooks))
                return ret;
            if (new_extent_hooks != &ehooks_default_extent_hooks)
                return EINVAL;
            ArenaConfig config = arena_config_default;
            config.extent_hooks = new_extent_hooks;
            if (arenaInit(&tsd, arena_ind, &config) == nullptr)
                return EFAULT;
        }
    }
    else
    {
        if (newp != nullptr)
        {
            extent_hooks_t * new_extent_hooks = nullptr;
            if (int ret = write(newp, newlen, new_extent_hooks))
                return ret;
            if (new_extent_hooks != &ehooks_default_extent_hooks)
                return EINVAL;
            /// jemalloc: arena_set_extent_hooks (installing the default table again does not change anything).
            old_extent_hooks = arenaGetEhooks(arena)->getExtentHooksPtr();
            if (int ret = read(oldp, oldlenp, old_extent_hooks))
                return ret;
        }
        else
        {
            old_extent_hooks = arenaGetEhooks(arena)->getExtentHooksPtr();
            if (int ret = read(oldp, oldlenp, old_extent_hooks))
                return ret;
        }
    }
    return 0;
}

/// Only exists with `opt.retain`.
/// jemalloc: arena_i_retain_grow_limit_ctl
int arenaIRetainGrowLimit(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!opt.retain)
    {
        /// Only relevant when retain is enabled.
        return ENOENT;
    }

    MutexLock lock(&tsd, ctl_mtx);
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    Arena * arena;
    if (arena_ind < narenasTotalGet() && (arena = arenaGet(&tsd, arena_ind, false)) != nullptr)
    {
        size_t old_limit;
        size_t new_limit;
        if (newp != nullptr)
        {
            if (int ret = write(newp, newlen, new_limit))
                return ret;
        }
        bool err = arenaRetainGrowLimitGetSet(tsd, arena, &old_limit, newp != nullptr ? &new_limit : nullptr);
        if (err)
            return EFAULT;
        return read(oldp, oldlenp, old_limit);
    }
    return EFAULT;
}

/// When writing, `newp` points to a `char *` (a name longer than `ARENA_NAME_LEN` is cut). When reading, `oldp`
/// points to a `char *` buffer of at least `ARENA_NAME_LEN` bytes (or the length of the name when it was set).
/// jemalloc: arena_i_name_ctl
int arenaIName(ThreadState & tsd, const size_t * mib, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    unsigned arena_ind;
    if (int ret = mibUnsigned(mib, 1, arena_ind))
        return ret;
    if (arena_ind == MALLCTL_ARENAS_ALL || arena_ind >= ctl_arenas->narenas)
        return EINVAL;
    Arena * arena = arenaGet(&tsd, arena_ind, false);
    if (arena == nullptr)
        return EFAULT;

    if (oldp != nullptr && oldlenp != nullptr)
    {
        /// Read the arena name.
        if (*oldlenp != sizeof(char *))
            return EINVAL;
        char * name = *static_cast<char **>(oldp);
        arenaNameGet(arena, name);
    }

    if (newp != nullptr)
    {
        /// Write the arena name.
        char * name = nullptr;
        if (int ret = write(newp, newlen, name))
            return ret;
        if (name == nullptr)
            return EINVAL;
        arenaNameSet(arena, name);
    }
    return 0;
}

/// The ctl's count of arenas: `narenas_total_get()` at initialization, incremented only by `arenas.create`.
/// jemalloc: arenas_narenas_ctl
int arenasNarenas(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    if (int ret = readOnly(newp, newlen))
        return ret;
    unsigned narenas = ctl_arenas->narenas;
    return read(oldp, oldlenp, narenas);
}

/// Affects arenas created afterwards only.
/// jemalloc: arenas_dirty_decay_ms_ctl
int arenasDirtyDecayMs(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return arenasDecayMsImpl(oldp, oldlenp, newp, newlen, true);
}

/// jemalloc: arenas_muzzy_decay_ms_ctl
int arenasMuzzyDecayMs(ThreadState &, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return arenasDecayMsImpl(oldp, oldlenp, newp, newlen, false);
}

/// jemalloc: CTL_RO_NL_GEN(arenas_tcache_max, global_do_not_change_tcache_maxclass, size_t)
int arenasTcacheMax(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyNl<size_t, [] { return global_do_not_change_tcache_maxclass; }>(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// jemalloc: CTL_RO_NL_GEN(arenas_nhbins, global_do_not_change_tcache_nbins, unsigned)
int arenasNhbins(ThreadState & tsd, const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    return readOnlyNl<unsigned, [] { return global_do_not_change_tcache_nbins; }>(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// Custom extent hooks are not supported: writing any other table than `ehooks_default_extent_hooks` returns
/// `EINVAL` (see `arena.<i>.extent_hooks`).
/// jemalloc: arenas_create_ctl
int arenasCreate(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);

    if (int ret = verifyRead<unsigned>(oldp, oldlenp))
        return ret;
    ArenaConfig config = arena_config_default;
    extent_hooks_t * extent_hooks = const_cast<extent_hooks_t *>(config.extent_hooks);
    if (int ret = write(newp, newlen, extent_hooks))
        return ret;
    if (extent_hooks != &ehooks_default_extent_hooks)
        return EINVAL;
    config.extent_hooks = extent_hooks;
    unsigned arena_ind = ctlArenaInit(tsd, &config);
    if (arena_ind == UINT_MAX)
        return EAGAIN;
    return read(oldp, oldlenp, arena_ind);
}

/// jemalloc: arenas_lookup_ctl
int arenasLookup(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    MutexLock lock(&tsd, ctl_mtx);
    void * ptr = nullptr;
    if (int ret = write(newp, newlen, ptr))
        return ret;
    FullAllocContext alloc_ctx;
    bool ptr_not_present = arena_emap_global.fullAllocCtxTryLookup(&tsd, ptr, &alloc_ctx);
    if (ptr_not_present || alloc_ctx.edata == nullptr)
        return EINVAL;

    Arena * arena = arenaGetFromEdata(alloc_ctx.edata);
    if (arena == nullptr)
        return EINVAL;

    unsigned arena_ind = arenaIndGet(arena);
    return read(oldp, oldlenp, arena_ind);
}

}
