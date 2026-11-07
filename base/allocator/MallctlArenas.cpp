/// `arena.<i>.*` and `arenas.*` (jemalloc: `ctl.c`). The constant `arenas.*` leaves and the index functions of
/// `arenas.bin` and `arenas.lextent` are generated in MallctlTree.cpp; the index function of `arena` is in Mallctl.cpp.

#include <allocator/MallctlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc::mallctl
{

/// The value reflects the last `epoch` refresh (except `arena.<i>.destroy`, which updates it immediately).
/// jemalloc: arena_i_initialized_ctl
int arenaIInitialized(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;

    bool initialized;
    {
        MutexLock lock(&thread_state, mallctl_mutex);
        initialized = arenasI(arena_idx)->initialized;
    }
    return read(old_value, old_length_ptr, initialized);
}

namespace
{

/// `arena_idx` is the index of `arena.<i>`: `MALLCTL_ARENAS_ALL` (or the deprecated alias `num_arenas`) decays every
/// arena.
/// jemalloc: arena_i_decay
void arenaIDecayImpl(ThreadState * thread_state, unsigned arena_idx, bool all)
{
    mallctl_mutex.lock(thread_state);
    unsigned num_arenas = mallctl_arenas->num_arenas;

    /// Access via index narenas is deprecated, and scheduled for removal in 6.0.0.
    if (arena_idx == MALLCTL_ARENAS_ALL || arena_idx == num_arenas)
    {
        Arena * all_arenas[MALLOCX_ARENA_LIMIT];
        for (unsigned i = 0; i < num_arenas; ++i)
            all_arenas[i] = arenaGet(thread_state, i, false);

        /// No further need to hold ctl_mtx, since narenas and tarenas contain everything needed below.
        mallctl_mutex.unlock(thread_state);

        for (unsigned i = 0; i < num_arenas; ++i)
        {
            if (all_arenas[i] != nullptr)
                arenaDecay(thread_state, all_arenas[i], false, all);
        }
    }
    else
    {
        /// jemalloc reads `arenas[4097]` for `MALLCTL_ARENAS_DESTROYED` (out of bounds, undefined behavior); here it
        /// is a no-op.
        Arena * this_arena = (arena_idx < num_arenas) ? arenaGet(thread_state, arena_idx, false) : nullptr;

        /// No further need to hold ctl_mtx.
        mallctl_mutex.unlock(thread_state);

        if (this_arena != nullptr)
            arenaDecay(thread_state, this_arena, false, all);
    }
}

/// jemalloc: arena_i_reset_destroy_helper
int arenaIResetDestroyHelper(
    ThreadState & thread_state,
    const size_t * numeric_path,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length,
    unsigned & arena_idx,
    Arena *& arena)
{
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;

    /// jemalloc reads `arenas[4096]` / `arenas[4097]` (out of bounds) for the merged slots; here they do not exist.
    arena = arena_idx < MALLOCX_ARENA_LIMIT ? arenaGet(&thread_state, arena_idx, false) : nullptr;
    if (arena == nullptr || arenaIsAuto(arena))
        return EFAULT;
    return 0;
}

/// Temporarily disable the background thread during arena reset (`background_thread_lock` stays locked).
/// jemalloc: arena_reset_prepare_background_thread
void arenaResetPrepareBackgroundThread(ThreadState & thread_state, unsigned arena_idx)
{
    if constexpr (config::background_thread)
    {
        background_thread_lock.lock(&thread_state);
        if (backgroundThreadEnabled())
        {
            BackgroundThreadInfo * info = backgroundThreadInfoGet(arena_idx);
            ALLOCATOR_ASSERT(info->state == BackgroundThreadState::Started);
            MutexLock lock(&thread_state, info->mutex);
            info->state = BackgroundThreadState::Paused;
        }
    }
}

/// jemalloc: arena_reset_finish_background_thread
void arenaResetFinishBackgroundThread(ThreadState & thread_state, unsigned arena_idx)
{
    if constexpr (config::background_thread)
    {
        if (backgroundThreadEnabled())
        {
            BackgroundThreadInfo * info = backgroundThreadInfoGet(arena_idx);
            ALLOCATOR_ASSERT(info->state == BackgroundThreadState::Paused);
            MutexLock lock(&thread_state, info->mutex);
            info->state = BackgroundThreadState::Started;
        }
        background_thread_lock.unlock(&thread_state);
    }
}

/// jemalloc: arena_i_decay_ms_ctl_impl
int arenaIDecayMsImpl(
    ThreadState & thread_state,
    const size_t * numeric_path,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length,
    bool dirty)
{
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    /// jemalloc reads out of bounds for the merged slots (4096, 4097); here they do not exist.
    Arena * arena = arena_idx < MALLOCX_ARENA_LIMIT ? arenaGet(&thread_state, arena_idx, false) : nullptr;
    if (arena == nullptr)
        return EFAULT;
    ExtentState state = dirty ? extent_state_dirty : extent_state_muzzy;

    if (old_value != nullptr && old_length_ptr != nullptr)
    {
        ssize_t old_setting = arenaDecayMsGet(arena, state);
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;
    }
    if (new_value != nullptr)
    {
        if (new_length != sizeof(ssize_t))
            return EINVAL;
        if (arenaDecayMsSet(&thread_state, arena, state, *static_cast<const ssize_t *>(new_value)))
            return EFAULT;
    }
    return 0;
}

/// jemalloc: arenas_decay_ms_ctl_impl
int arenasDecayMsImpl(void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length, bool dirty)
{
    if (old_value != nullptr && old_length_ptr != nullptr)
    {
        ssize_t old_setting = dirty ? arenaDirtyDecayMsDefaultGet() : arenaMuzzyDecayMsDefaultGet();
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;
    }
    if (new_value != nullptr)
    {
        if (new_length != sizeof(ssize_t))
            return EINVAL;
        ssize_t new_setting = *static_cast<const ssize_t *>(new_value);
        if (dirty ? arenaDirtyDecayMsDefaultSet(new_setting) : arenaMuzzyDecayMsDefaultSet(new_setting))
            return EFAULT;
    }
    return 0;
}

}

/// jemalloc: arena_i_decay_ctl
int arenaIDecay(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    arenaIDecayImpl(&thread_state, arena_idx, false);
    return 0;
}

/// jemalloc: arena_i_purge_ctl
int arenaIPurge(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    arenaIDecayImpl(&thread_state, arena_idx, true);
    return 0;
}

/// Only manual arenas can be reset.
/// jemalloc: arena_i_reset_ctl
int arenaIReset(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    unsigned arena_idx;
    Arena * arena;
    if (int result = arenaIResetDestroyHelper(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length, arena_idx, arena))
        return result;

    arenaResetPrepareBackgroundThread(thread_state, arena_idx);
    arenaReset(thread_state, arena);
    arenaResetFinishBackgroundThread(thread_state, arena_idx);
    return 0;
}

/// Only manual arenas without threads can be destroyed. The stats are merged into the `MALLCTL_ARENAS_DESTROYED`
/// slot and the index is recycled by `arenas.create`.
/// jemalloc: arena_i_destroy_ctl
int arenaIDestroy(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);

    unsigned arena_idx;
    Arena * arena;
    if (int result = arenaIResetDestroyHelper(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length, arena_idx, arena))
        return result;

    if (arenaNumThreadsGet(arena, false) != 0 || arenaNumThreadsGet(arena, true) != 0)
        return EFAULT;

    arenaResetPrepareBackgroundThread(thread_state, arena_idx);
    /// Merge stats after resetting and purging arena.
    arenaReset(thread_state, arena);
    arenaDecay(&thread_state, arena, false, true);
    MallctlArena * mallctl_destroyed_arena = arenasI(MALLCTL_ARENAS_DESTROYED);
    mallctl_destroyed_arena->initialized = true;
    mallctlArenaRefresh(&thread_state, arena, mallctl_destroyed_arena, arena_idx, true);
    /// Destroy arena.
    arenaDestroy(thread_state, arena);
    MallctlArena * mallctl_arena = arenasI(arena_idx);
    mallctl_arena->initialized = false;
    /// Record arena index for later recycling via arenas.create.
    decltype(mallctl_arenas->destroyed)::elementInit(mallctl_arena);
    mallctl_arenas->destroyed.tailInsert(mallctl_arena);
    arenaResetFinishBackgroundThread(thread_state, arena_idx);
    return 0;
}

/// DSS is dropped, but the precedence settings are stored and reported. Note that the returned "old" value is read
/// after the set (so it is the new setting).
/// jemalloc: arena_i_dss_ctl
int arenaISbrk(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    const char * sbrk = nullptr;
    if (int result = write(new_value, new_length, sbrk))
        return result;
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;

    SbrkPrecedence sbrk_precedence = SbrkPrecedence::Limit;
    if (sbrk != nullptr)
    {
        bool match = false;
        for (unsigned i = 0; i < unsigned(SbrkPrecedence::Limit); ++i)
        {
            if (std::strcmp(sbrk_precedence_names[i], sbrk) == 0)
            {
                sbrk_precedence = SbrkPrecedence(i);
                match = true;
                break;
            }
        }
        if (!match)
            return EINVAL;
    }

    /// Access via index narenas is deprecated, and scheduled for removal in 6.0.0.
    SbrkPrecedence sbrk_precedence_old;
    if (arena_idx == MALLCTL_ARENAS_ALL || arena_idx == mallctl_arenas->num_arenas)
    {
        if (sbrk_precedence != SbrkPrecedence::Limit && extentSbrkPrecedenceSet(sbrk_precedence))
            return EFAULT;
        sbrk_precedence_old = extentSbrkPrecedenceGet();
    }
    else
    {
        /// jemalloc reads `arenas[4097]` (out of bounds) for `MALLCTL_ARENAS_DESTROYED`; here it does not exist.
        Arena * arena = arena_idx < MALLOCX_ARENA_LIMIT ? arenaGet(&thread_state, arena_idx, false) : nullptr;
        if (arena == nullptr || (sbrk_precedence != SbrkPrecedence::Limit && arenaSbrkPrecedenceSet(arena, sbrk_precedence)))
            return EFAULT;
        sbrk_precedence_old = arenaSbrkPrecedenceGet(arena);
    }

    sbrk = sbrk_precedence_names[unsigned(sbrk_precedence_old)];
    return read(old_value, old_length_ptr, sbrk);
}

/// No validation of the value.
/// jemalloc: arena_i_oversize_threshold_ctl
int arenaIOversizeThreshold(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;

    /// jemalloc reads out of bounds for the merged slots (4096, 4097); here they do not exist.
    Arena * arena = arena_idx < MALLOCX_ARENA_LIMIT ? arenaGet(&thread_state, arena_idx, false) : nullptr;
    if (arena == nullptr)
        return EFAULT;

    if (old_value != nullptr && old_length_ptr != nullptr)
    {
        size_t old_setting = arena->page_allocator_shard.page_allocator.oversize_threshold.load(std::memory_order_relaxed);
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;
    }
    if (new_value != nullptr)
    {
        if (new_length != sizeof(size_t))
            return EINVAL;
        arena->page_allocator_shard.page_allocator.oversize_threshold.store(
            *static_cast<const size_t *>(new_value), std::memory_order_relaxed);
    }
    return 0;
}

/// jemalloc: arena_i_dirty_decay_ms_ctl
int arenaIDirtyDecayMs(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return arenaIDecayMsImpl(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length, true);
}

/// jemalloc: arena_i_muzzy_decay_ms_ctl
int arenaIMuzzyDecayMs(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return arenaIDecayMsImpl(thread_state, numeric_path, old_value, old_length_ptr, new_value, new_length, false);
}

/// Custom extent hooks are not supported (the default hooks are always used): writing any other table than
/// `extent_hooks_default_extent_hooks` returns `EINVAL` (jemalloc would install it). Otherwise as in jemalloc: writing to
/// a missing auto arena creates it.
/// jemalloc: arena_i_extent_hooks_ctl
int arenaIExtentHooks(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    if (arena_idx >= numArenasTotalGet())
        return EFAULT;

    Arena * arena = arenaGet(&thread_state, arena_idx, false);
    extent_hooks_t * old_extent_hooks_ptr;
    if (arena == nullptr)
    {
        if (arena_idx >= num_arenas_auto)
            return EFAULT;
        old_extent_hooks_ptr = const_cast<extent_hooks_t *>(&extent_hooks_default_extent_hooks);
        if (int result = read(old_value, old_length_ptr, old_extent_hooks_ptr))
            return result;
        if (new_value != nullptr)
        {
            /// Initialize a new arena as a side effect.
            extent_hooks_t * new_extent_hooks_ptr = nullptr;
            if (int result = write(new_value, new_length, new_extent_hooks_ptr))
                return result;
            if (new_extent_hooks_ptr != &extent_hooks_default_extent_hooks)
                return EINVAL;
            ArenaConfig config = arena_config_default;
            config.extent_hooks_ptr = new_extent_hooks_ptr;
            if (arenaInit(&thread_state, arena_idx, &config) == nullptr)
                return EFAULT;
        }
    }
    else
    {
        if (new_value != nullptr)
        {
            extent_hooks_t * new_extent_hooks_ptr = nullptr;
            if (int result = write(new_value, new_length, new_extent_hooks_ptr))
                return result;
            if (new_extent_hooks_ptr != &extent_hooks_default_extent_hooks)
                return EINVAL;
            /// jemalloc: arena_set_extent_hooks (installing the default table again does not change anything).
            old_extent_hooks_ptr = arenaGetExtentHooks(arena)->getExtentHooksPtr();
            if (int result = read(old_value, old_length_ptr, old_extent_hooks_ptr))
                return result;
        }
        else
        {
            old_extent_hooks_ptr = arenaGetExtentHooks(arena)->getExtentHooksPtr();
            if (int result = read(old_value, old_length_ptr, old_extent_hooks_ptr))
                return result;
        }
    }
    return 0;
}

/// Only exists with `opt.retain`.
/// jemalloc: arena_i_retain_grow_limit_ctl
int arenaIRetainGrowLimit(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!options.retain)
    {
        /// Only relevant when retain is enabled.
        return ENOENT;
    }

    MutexLock lock(&thread_state, mallctl_mutex);
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    Arena * arena;
    if (arena_idx < numArenasTotalGet() && (arena = arenaGet(&thread_state, arena_idx, false)) != nullptr)
    {
        size_t old_limit;
        size_t new_limit;
        if (new_value != nullptr)
        {
            if (int result = write(new_value, new_length, new_limit))
                return result;
        }
        bool error = arenaRetainGrowLimitGetSet(thread_state, arena, &old_limit, new_value != nullptr ? &new_limit : nullptr);
        if (error)
            return EFAULT;
        return read(old_value, old_length_ptr, old_limit);
    }
    return EFAULT;
}

/// When writing, `new_value` points to a `char *` (a name longer than `ARENA_NAME_LEN` is cut). When reading, `old_value`
/// points to a `char *` buffer of at least `ARENA_NAME_LEN` bytes (or the length of the name when it was set).
/// jemalloc: arena_i_name_ctl
int arenaIName(
    ThreadState & thread_state, const size_t * numeric_path, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    unsigned arena_idx;
    if (int result = numericPathComponentUnsigned(numeric_path, 1, arena_idx))
        return result;
    if (arena_idx == MALLCTL_ARENAS_ALL || arena_idx >= mallctl_arenas->num_arenas)
        return EINVAL;
    Arena * arena = arenaGet(&thread_state, arena_idx, false);
    if (arena == nullptr)
        return EFAULT;

    if (old_value != nullptr && old_length_ptr != nullptr)
    {
        /// Read the arena name.
        if (*old_length_ptr != sizeof(char *))
            return EINVAL;
        char * name = *static_cast<char **>(old_value);
        arenaNameGet(arena, name);
    }

    if (new_value != nullptr)
    {
        /// Write the arena name.
        char * name = nullptr;
        if (int result = write(new_value, new_length, name))
            return result;
        if (name == nullptr)
            return EINVAL;
        arenaNameSet(arena, name);
    }
    return 0;
}

/// The ctl's count of arenas: `narenas_total_get()` at initialization, incremented only by `arenas.create`.
/// jemalloc: arenas_narenas_ctl
int arenasNumArenas(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    if (int result = readOnly(new_value, new_length))
        return result;
    unsigned num_arenas = mallctl_arenas->num_arenas;
    return read(old_value, old_length_ptr, num_arenas);
}

/// Affects arenas created afterwards only.
/// jemalloc: arenas_dirty_decay_ms_ctl
int arenasDirtyDecayMs(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return arenasDecayMsImpl(old_value, old_length_ptr, new_value, new_length, true);
}

/// jemalloc: arenas_muzzy_decay_ms_ctl
int arenasMuzzyDecayMs(
    ThreadState &, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return arenasDecayMsImpl(old_value, old_length_ptr, new_value, new_length, false);
}

/// jemalloc: CTL_RO_NL_GEN(arenas_tcache_max, global_do_not_change_tcache_maxclass, size_t)
int arenasThreadCacheMax(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyNoLock<size_t, [] { return global_do_not_change_thread_cache_max_class; }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: CTL_RO_NL_GEN(arenas_nhbins, global_do_not_change_tcache_nbins, unsigned)
int arenasNumThreadCacheBins(
    ThreadState & thread_state,
    const size_t * numeric_path,
    size_t numeric_path_length,
    void * old_value,
    size_t * old_length_ptr,
    void * new_value,
    size_t new_length)
{
    return readOnlyNoLock<unsigned, [] { return global_do_not_change_thread_cache_num_bins; }>(
        thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// Custom extent hooks are not supported: writing any other table than `extent_hooks_default_extent_hooks` returns
/// `EINVAL` (see `arena.<i>.extent_hooks`).
/// jemalloc: arenas_create_ctl
int arenasCreate(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);

    if (int result = verifyRead<unsigned>(old_value, old_length_ptr))
        return result;
    ArenaConfig config = arena_config_default;
    extent_hooks_t * extent_hooks_ptr = const_cast<extent_hooks_t *>(config.extent_hooks_ptr);
    if (int result = write(new_value, new_length, extent_hooks_ptr))
        return result;
    if (extent_hooks_ptr != &extent_hooks_default_extent_hooks)
        return EINVAL;
    config.extent_hooks_ptr = extent_hooks_ptr;
    unsigned arena_idx = mallctlArenaInit(thread_state, &config);
    if (arena_idx == UINT_MAX)
        return EAGAIN;
    return read(old_value, old_length_ptr, arena_idx);
}

/// jemalloc: arenas_lookup_ctl
int arenasLookup(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    MutexLock lock(&thread_state, mallctl_mutex);
    void * ptr = nullptr;
    if (int result = write(new_value, new_length, ptr))
        return result;
    FullAllocContext alloc_context;
    bool ptr_not_present = arena_extent_map_global.fullAllocContextTryLookup(&thread_state, ptr, &alloc_context);
    if (ptr_not_present || alloc_context.extent == nullptr)
        return EINVAL;

    Arena * arena = arenaGetFromExtent(alloc_context.extent);
    if (arena == nullptr)
        return EINVAL;

    unsigned arena_idx = arenaIdxGet(arena);
    return read(old_value, old_length_ptr, arena_idx);
}

}
