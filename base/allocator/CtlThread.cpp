/// `background_thread`, `max_background_threads`, `thread.*`, `tcache.*` (jemalloc: `ctl.c`).

#include <allocator/CtlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Options.h>
#include <allocator/Prof.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc::ctl
{

/// Takes `ctl_mtx`, then `background_thread_lock`. Writing true starts the threads, writing false stops and joins
/// them (`EFAULT` on failure).
/// jemalloc: background_thread_ctl
int backgroundThread(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::background_thread)
        return ENOENT;
    backgroundThreadCtlInit(&tsd);

    MutexLock ctl_lock(&tsd, ctl_mtx);
    MutexLock background_lock(&tsd, background_thread_lock);
    bool oldval;
    if (newp == nullptr)
    {
        oldval = backgroundThreadEnabled();
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;
    }
    else
    {
        if (newlen != sizeof(bool))
            return EINVAL;
        oldval = backgroundThreadEnabled();
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;

        bool newval = *static_cast<bool *>(newp);
        if (newval == oldval)
            return 0;

        backgroundThreadEnabledSet(&tsd, newval);
        if (newval)
        {
            if (backgroundThreadsEnable(tsd))
                return EFAULT;
        }
        else
        {
            if (backgroundThreadsDisable(tsd))
                return EFAULT;
        }
    }
    return 0;
}

/// The new value must be in `[1, opt.max_background_threads]`; running threads are stopped and restarted with the new
/// count.
/// jemalloc: max_background_threads_ctl
int maxBackgroundThreads(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::background_thread)
        return ENOENT;
    backgroundThreadCtlInit(&tsd);

    MutexLock ctl_lock(&tsd, ctl_mtx);
    MutexLock background_lock(&tsd, background_thread_lock);
    size_t oldval;
    if (newp == nullptr)
    {
        oldval = max_background_threads;
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;
    }
    else
    {
        if (newlen != sizeof(size_t))
            return EINVAL;
        oldval = max_background_threads;
        if (int ret = read(oldp, oldlenp, oldval))
            return ret;

        size_t newval = *static_cast<size_t *>(newp);
        if (newval == oldval)
            return 0;
        if (newval > opt.max_background_threads || newval == 0)
            return EINVAL;

        if (backgroundThreadEnabled())
        {
            backgroundThreadEnabledSet(&tsd, false);
            if (backgroundThreadsDisable(tsd))
                return EFAULT;
            max_background_threads = newval;
            backgroundThreadEnabledSet(&tsd, true);
            if (backgroundThreadsEnable(tsd))
                return EFAULT;
        }
        else
        {
            max_background_threads = newval;
        }
    }
    return 0;
}

/// ClickHouse fork patch (77f09068, fe67fff6): with per-CPU arenas, writing an index in the per-CPU range means
/// "resume the automatic per-CPU selection" (the thread is bound to the arena of the current CPU, not necessarily the
/// written one) instead of `EPERM`.
/// jemalloc: thread_arena_ctl
int threadArena(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    Arena * oldarena = arenaChoose(tsd, nullptr);
    if (oldarena == nullptr)
        return EAGAIN;
    unsigned oldind = arenaIndGet(oldarena);
    unsigned newind = oldind;
    if (int ret = write(newp, newlen, newind))
        return ret;
    if (int ret = read(oldp, oldlenp, oldind))
        return ret;

    if (newind != oldind)
    {
        if (newind >= narenasTotalGet())
        {
            /// New arena index is out of range.
            return EFAULT;
        }

        if (config::have_percpu_arena && percpuArenaEnabled(opt.percpu_arena))
        {
            if (newind < percpuArenaIndLimit(opt.percpu_arena))
            {
                /// Setting `thread.arena` to an arena in the auto range means "resume automatic per-CPU selection"
                /// rather than pinning to a specific per-CPU arena: a thread bound to a manual arena is never
                /// reclaimed by percpu (see `arenaChooseImpl`), so without this it would stay pinned forever.
                percpuArenaUpdate(tsd, percpuArenaChoose());
                return 0;
            }
        }

        /// Initialize arena if necessary.
        Arena * newarena = arenaGet(&tsd, newind, true);
        if (newarena == nullptr)
            return EAGAIN;
        /// Set new arena/tcache associations.
        arenaMigrate(tsd, oldarena, newarena);
        if (tcacheAvailable(tsd))
            tcacheArenaReassociate(&tsd, tsd.tcacheSlowGet(), tsd.tcacheGet(), newarena);
    }
    return 0;
}

/// jemalloc: CTL_RO_NL_GEN(thread_allocated, tsd_thread_allocated_get(tsd), uint64_t)
int threadAllocated(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    uint64_t oldval = tsd.thread_allocated;
    return read(oldp, oldlenp, oldval);
}

/// The address is stable for the lifetime of the thread.
/// jemalloc: CTL_RO_NL_GEN(thread_allocatedp, tsd_thread_allocatedp_get(tsd), uint64_t *)
int threadAllocatedp(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    uint64_t * oldval = &tsd.thread_allocated;
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: CTL_RO_NL_GEN(thread_deallocated, tsd_thread_deallocated_get(tsd), uint64_t)
int threadDeallocated(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    uint64_t oldval = tsd.thread_deallocated;
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: CTL_RO_NL_GEN(thread_deallocatedp, tsd_thread_deallocatedp_get(tsd), uint64_t *)
int threadDeallocatedp(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    uint64_t * oldval = &tsd.thread_deallocated;
    return read(oldp, oldlenp, oldval);
}

/// The new value is applied before the old one is read back (a read size error is reported after the change).
/// jemalloc: thread_tcache_enabled_ctl
int threadTcacheEnabled(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    bool oldval = tcacheEnabledGet(tsd);
    if (newp != nullptr)
    {
        if (newlen != sizeof(bool))
            return EINVAL;
        tcacheEnabledSet(tsd, *static_cast<const bool *>(newp));
    }
    return read(oldp, oldlenp, oldval);
}

/// The new value is clipped to `TCACHE_MAXCLASS_LIMIT` and rounded up to a size class.
/// jemalloc: thread_tcache_max_ctl
int threadTcacheMax(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    /// The pointer to the tcache always exists even with tcache disabled.
    ThreadCache * tcache = tsd.tcacheGet();
    JE_ASSERT(tcache != nullptr);
    size_t oldval = tcacheMaxGet(tcache->tcache_slow);
    if (int ret = read(oldp, oldlenp, oldval))
        return ret;

    if (newp != nullptr)
    {
        if (newlen != sizeof(size_t))
            return EINVAL;
        size_t new_tcache_max = oldval;
        if (int ret = write(newp, newlen, new_tcache_max))
            return ret;
        if (new_tcache_max > TCACHE_MAXCLASS_LIMIT)
            new_tcache_max = TCACHE_MAXCLASS_LIMIT;
        new_tcache_max = sz::s2u(new_tcache_max);
        if (new_tcache_max != oldval)
            threadTcacheMaxSet(tsd, new_tcache_max);
    }
    return 0;
}

/// jemalloc: thread_tcache_flush_ctl
int threadTcacheFlush(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!tcacheAvailable(tsd))
        return EFAULT;
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;
    tcacheFlush(tsd);
    return 0;
}

/// The bin size is passed in `newp`; the result is a `size_t`.
/// jemalloc: thread_tcache_ncached_max_read_sizeclass_ctl
int threadTcacheNcachedMaxReadSizeclass(
    ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    /// Read the bin size from newp.
    if (newp == nullptr)
        return EINVAL;
    size_t bin_size = 0;
    if (int ret = write(newp, newlen, bin_size))
        return ret;

    cache_bin_sz_t ncached_max = 0;
    if (tcacheBinNcachedMaxRead(tsd, bin_size, ncached_max))
        return EINVAL;
    size_t result = size_t(ncached_max);
    return read(oldp, oldlenp, result);
}

/// `newp` points to a `char *` with `start-end:ncached_max[|...]` settings.
/// jemalloc: thread_tcache_ncached_max_write_ctl
int threadTcacheNcachedMaxWrite(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    if (newp != nullptr)
    {
        if (!tcacheAvailable(tsd))
            return ENOENT;
        char * settings = nullptr;
        if (int ret = write(newp, newlen, settings))
            return ret;
        if (settings == nullptr)
            return EINVAL;
        /// Get the length of the setting string safely.
        const char * end = static_cast<const char *>(std::memchr(settings, '\0', CTL_MULTI_SETTING_MAX_LEN));
        if (end == nullptr)
            return EINVAL;
        /// Exclude the last '\0' for len since it is not handled by `multiSettingParseNext`.
        size_t len = size_t(end - settings);
        if (len == 0)
            return 0;

        if (tcacheBinsNcachedMaxWrite(tsd, settings, len))
            return EINVAL;
    }
    return 0;
}

/// jemalloc: thread_peak_read_ctl
int threadPeakRead(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (int ret = readOnly(newp, newlen))
        return ret;
    peakEventUpdate(tsd);
    uint64_t result = peakEventMax(tsd);
    return read(oldp, oldlenp, result);
}

/// jemalloc: thread_peak_reset_ctl
int threadPeakReset(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;
    peakEventZero(tsd);
    return 0;
}

/// jemalloc: thread_prof_name_ctl
int threadProfName(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (!(config::prof && opt.prof))
        return ENOENT;

    if (int ret = readXorWrite(oldp, oldlenp, newp, newlen))
        return ret;

    if (newp != nullptr)
    {
        const char * newval = *static_cast<const char **>(newp);
        if (newlen != sizeof(const char *) || newval == nullptr)
            return EINVAL;

        if (int ret = profThreadNameSet(tsd, newval))
            return ret;
    }
    else
    {
        const char * oldname = profThreadNameGet(tsd);
        if (int ret = read(oldp, oldlenp, oldname))
            return ret;
    }
    return 0;
}

/// jemalloc: thread_prof_active_ctl
int threadProfActive(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if constexpr (!config::prof)
        return ENOENT;

    bool oldval = opt.prof ? profThreadActiveGet(tsd) : false;
    if (newp != nullptr)
    {
        if (!opt.prof)
            return ENOENT;
        if (newlen != sizeof(bool))
            return EINVAL;
        if (profThreadActiveSet(tsd, *static_cast<bool *>(newp)))
            return EAGAIN;
    }
    return read(oldp, oldlenp, oldval);
}

/// jemalloc: thread_idle_ctl
int threadIdle(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = neitherReadNorWrite(oldp, oldlenp, newp, newlen))
        return ret;

    if (tcacheAvailable(tsd))
        tcacheFlush(tsd);
    /// This heuristic is perhaps not the most well-considered. But it matches the only idling policy we have
    /// experience with in the status quo. Over time we should investigate more principled approaches.
    if (opt.narenas > ncpus * 2)
    {
        Arena * arena = arenaChoose(tsd, nullptr);
        if (arena != nullptr)
            arenaDecay(&tsd, arena, false, true);
        /// The missing arena case is not actually an error; a thread might be idle before it associates itself to
        /// one. This is unusual, but not wrong.
    }
    return 0;
}

/// jemalloc: tcache_create_ctl
int tcacheCreate(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = readOnly(newp, newlen))
        return ret;
    if (int ret = verifyRead<unsigned>(oldp, oldlenp))
        return ret;
    unsigned tcache_ind;
    if (tcachesCreate(tsd, b0get(), tcache_ind))
        return EFAULT;
    return read(oldp, oldlenp, tcache_ind);
}

/// No range check here (the callee handles it).
/// jemalloc: tcache_flush_ctl
int tcacheFlush(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    unsigned tcache_ind;
    if (int ret = assuredWrite(newp, newlen, tcache_ind))
        return ret;
    tcachesFlush(tsd, tcache_ind);
    return 0;
}

/// jemalloc: tcache_destroy_ctl
int tcacheDestroy(ThreadState & tsd, const size_t *, size_t, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (int ret = writeOnly(oldp, oldlenp))
        return ret;
    unsigned tcache_ind;
    if (int ret = assuredWrite(newp, newlen, tcache_ind))
        return ret;
    tcachesDestroy(tsd, tcache_ind);
    return 0;
}

}
