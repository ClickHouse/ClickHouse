/// `background_thread`, `max_background_threads`, `thread.*`, `tcache.*` (jemalloc: `ctl.c`).

#include <allocator/MallctlImpl.h>

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/Options.h>
#include <allocator/Profiling.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc::mallctl
{

/// Takes `mallctl_mutex`, then `background_thread_lock`. Writing true starts the threads, writing false stops and joins
/// them (`EFAULT` on failure).
/// jemalloc: background_thread_ctl
int backgroundThread(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::background_thread)
        return ENOENT;
    backgroundThreadMallctlInit(&thread_state);

    MutexLock mallctl_lock(&thread_state, mallctl_mutex);
    MutexLock background_lock(&thread_state, background_thread_lock);
    bool old_setting;
    if (new_value == nullptr)
    {
        old_setting = backgroundThreadEnabled();
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;
    }
    else
    {
        if (new_length != sizeof(bool))
            return EINVAL;
        old_setting = backgroundThreadEnabled();
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;

        bool new_setting = *static_cast<bool *>(new_value);
        if (new_setting == old_setting)
            return 0;

        backgroundThreadEnabledSet(&thread_state, new_setting);
        if (new_setting)
        {
            if (backgroundThreadsEnable(thread_state))
                return EFAULT;
        }
        else
        {
            if (backgroundThreadsDisable(thread_state))
                return EFAULT;
        }
    }
    return 0;
}

/// The new value must be in `[1, options.max_background_threads]`; running threads are stopped and restarted with the new
/// count.
/// jemalloc: max_background_threads_ctl
int maxBackgroundThreads(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::background_thread)
        return ENOENT;
    backgroundThreadMallctlInit(&thread_state);

    MutexLock mallctl_lock(&thread_state, mallctl_mutex);
    MutexLock background_lock(&thread_state, background_thread_lock);
    size_t old_setting;
    if (new_value == nullptr)
    {
        old_setting = max_background_threads;
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;
    }
    else
    {
        if (new_length != sizeof(size_t))
            return EINVAL;
        old_setting = max_background_threads;
        if (int result = read(old_value, old_length_ptr, old_setting))
            return result;

        size_t new_setting = *static_cast<size_t *>(new_value);
        if (new_setting == old_setting)
            return 0;
        if (new_setting > options.max_background_threads || new_setting == 0)
            return EINVAL;

        if (backgroundThreadEnabled())
        {
            backgroundThreadEnabledSet(&thread_state, false);
            if (backgroundThreadsDisable(thread_state))
                return EFAULT;
            max_background_threads = new_setting;
            backgroundThreadEnabledSet(&thread_state, true);
            if (backgroundThreadsEnable(thread_state))
                return EFAULT;
        }
        else
        {
            max_background_threads = new_setting;
        }
    }
    return 0;
}

/// ClickHouse fork patch (77f09068, fe67fff6): with per-CPU arenas, writing an index in the per-CPU range means
/// "resume the automatic per-CPU selection" (the thread is bound to the arena of the current CPU, not necessarily the
/// written one) instead of `EPERM`.
/// jemalloc: thread_arena_ctl
int threadArena(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    Arena * old_arena = arenaChoose(thread_state, nullptr);
    if (old_arena == nullptr)
        return EAGAIN;
    unsigned old_idx = arenaIdxGet(old_arena);
    unsigned new_idx = old_idx;
    if (int result = write(new_value, new_length, new_idx))
        return result;
    if (int result = read(old_value, old_length_ptr, old_idx))
        return result;

    if (new_idx != old_idx)
    {
        if (new_idx >= numArenasTotalGet())
        {
            /// New arena index is out of range.
            return EFAULT;
        }

        if (config::have_per_cpu_arena && perCPUArenaEnabled(options.per_cpu_arena))
        {
            if (new_idx < perCPUArenaIdxLimit(options.per_cpu_arena))
            {
                /// Setting `thread.arena` to an arena in the auto range means "resume automatic per-CPU selection"
                /// rather than pinning to a specific per-CPU arena: a thread bound to a manual arena is never
                /// reclaimed by percpu (see `arenaChooseImpl`), so without this it would stay pinned forever.
                perCPUArenaUpdate(thread_state, perCPUArenaChoose());
                return 0;
            }
        }

        /// Initialize arena if necessary.
        Arena * new_arena = arenaGet(&thread_state, new_idx, true);
        if (new_arena == nullptr)
            return EAGAIN;
        /// Set new arena/tcache associations.
        arenaMigrate(thread_state, old_arena, new_arena);
        if (threadCacheAvailable(thread_state))
            threadCacheArenaReassociate(&thread_state, thread_state.threadCacheSlowGet(), thread_state.threadCacheGet(), new_arena);
    }
    return 0;
}

/// jemalloc: CTL_RO_NL_GEN(thread_allocated, tsd_thread_allocated_get(tsd), uint64_t)
int threadAllocated(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    uint64_t old_setting = thread_state.thread_allocated;
    return read(old_value, old_length_ptr, old_setting);
}

/// The address is stable for the lifetime of the thread.
/// jemalloc: CTL_RO_NL_GEN(thread_allocatedp, tsd_thread_allocatedp_get(tsd), uint64_t *)
int threadAllocatedPtr(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    uint64_t * old_setting = &thread_state.thread_allocated;
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: CTL_RO_NL_GEN(thread_deallocated, tsd_thread_deallocated_get(tsd), uint64_t)
int threadDeallocated(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    uint64_t old_setting = thread_state.thread_deallocated;
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: CTL_RO_NL_GEN(thread_deallocatedp, tsd_thread_deallocatedp_get(tsd), uint64_t *)
int threadDeallocatedPtr(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    uint64_t * old_setting = &thread_state.thread_deallocated;
    return read(old_value, old_length_ptr, old_setting);
}

/// The new value is applied before the old one is read back (a read size error is reported after the change).
/// jemalloc: thread_tcache_enabled_ctl
int threadCacheEnabled(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    bool old_setting = threadCacheEnabledGet(thread_state);
    if (new_value != nullptr)
    {
        if (new_length != sizeof(bool))
            return EINVAL;
        threadCacheEnabledSet(thread_state, *static_cast<const bool *>(new_value));
    }
    return read(old_value, old_length_ptr, old_setting);
}

/// The new value is clipped to `THREAD_CACHE_MAX_CLASS_LIMIT` and rounded up to a size class.
/// jemalloc: thread_tcache_max_ctl
int threadCacheMax(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    /// The pointer to the tcache always exists even with tcache disabled.
    ThreadCache * thread_cache = thread_state.threadCacheGet();
    ALLOCATOR_ASSERT(thread_cache != nullptr);
    size_t old_setting = threadCacheMaxGet(thread_cache->thread_cache_slow);
    if (int result = read(old_value, old_length_ptr, old_setting))
        return result;

    if (new_value != nullptr)
    {
        if (new_length != sizeof(size_t))
            return EINVAL;
        size_t new_thread_cache_max = old_setting;
        if (int result = write(new_value, new_length, new_thread_cache_max))
            return result;
        if (new_thread_cache_max > THREAD_CACHE_MAX_CLASS_LIMIT)
            new_thread_cache_max = THREAD_CACHE_MAX_CLASS_LIMIT;
        new_thread_cache_max = size_classes::sizeToUsableSize(new_thread_cache_max);
        if (new_thread_cache_max != old_setting)
            threadThreadCacheMaxSet(thread_state, new_thread_cache_max);
    }
    return 0;
}

/// jemalloc: thread_tcache_flush_ctl
int threadThreadCacheFlush(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!threadCacheAvailable(thread_state))
        return EFAULT;
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;
    threadCacheFlush(thread_state);
    return 0;
}

/// The bin size is passed in `new_value`; the result is a `size_t`.
/// jemalloc: thread_tcache_ncached_max_read_sizeclass_ctl
int threadCacheNumCachedMaxReadSizeClass(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    /// Read the bin size from newp.
    if (new_value == nullptr)
        return EINVAL;
    size_t bin_size = 0;
    if (int result = write(new_value, new_length, bin_size))
        return result;

    CacheBinSize num_cached_max = 0;
    if (threadCacheBinNumCachedMaxRead(thread_state, bin_size, num_cached_max))
        return EINVAL;
    size_t result = size_t(num_cached_max);
    return read(old_value, old_length_ptr, result);
}

/// `new_value` points to a `char *` with `start-end:num_cached_max[|...]` settings.
/// jemalloc: thread_tcache_ncached_max_write_ctl
int threadCacheNumCachedMaxWrite(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    if (new_value != nullptr)
    {
        if (!threadCacheAvailable(thread_state))
            return ENOENT;
        char * settings = nullptr;
        if (int result = write(new_value, new_length, settings))
            return result;
        if (settings == nullptr)
            return EINVAL;
        /// Get the length of the setting string safely.
        const char * end = static_cast<const char *>(std::memchr(settings, '\0', MALLCTL_MULTI_SETTING_MAX_LEN));
        if (end == nullptr)
            return EINVAL;
        /// Exclude the last '\0' for len since it is not handled by `multiSettingParseNext`.
        size_t len = size_t(end - settings);
        if (len == 0)
            return 0;

        if (threadCacheBinsNumCachedMaxWrite(thread_state, settings, len))
            return EINVAL;
    }
    return 0;
}

/// jemalloc: thread_peak_read_ctl
int threadPeakRead(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (int result = readOnly(new_value, new_length))
        return result;
    peakEventUpdate(thread_state);
    uint64_t result = peakEventMax(thread_state);
    return read(old_value, old_length_ptr, result);
}

/// jemalloc: thread_peak_reset_ctl
int threadPeakReset(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::stats)
        return ENOENT;
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;
    peakEventZero(thread_state);
    return 0;
}

/// jemalloc: thread_prof_name_ctl
int threadProfilingName(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (!(config::profiling && options.profiling))
        return ENOENT;

    if (int result = readXorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;

    if (new_value != nullptr)
    {
        const char * new_setting = *static_cast<const char **>(new_value);
        if (new_length != sizeof(const char *) || new_setting == nullptr)
            return EINVAL;

        if (int result = profilingThreadNameSet(thread_state, new_setting))
            return result;
    }
    else
    {
        const char * old_name = profilingThreadNameGet(thread_state);
        if (int result = read(old_value, old_length_ptr, old_name))
            return result;
    }
    return 0;
}

/// jemalloc: thread_prof_active_ctl
int threadProfilingActive(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if constexpr (!config::profiling)
        return ENOENT;

    bool old_setting = options.profiling ? profilingThreadActiveGet(thread_state) : false;
    if (new_value != nullptr)
    {
        if (!options.profiling)
            return ENOENT;
        if (new_length != sizeof(bool))
            return EINVAL;
        if (profilingThreadActiveSet(thread_state, *static_cast<bool *>(new_value)))
            return EAGAIN;
    }
    return read(old_value, old_length_ptr, old_setting);
}

/// jemalloc: thread_idle_ctl
int threadIdle(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = neitherReadNorWrite(old_value, old_length_ptr, new_value, new_length))
        return result;

    if (threadCacheAvailable(thread_state))
        threadCacheFlush(thread_state);
    /// This heuristic is perhaps not the most well-considered. But it matches the only idling policy we have
    /// experience with in the status quo. Over time we should investigate more principled approaches.
    if (options.num_arenas > num_cpus * 2)
    {
        Arena * arena = arenaChoose(thread_state, nullptr);
        if (arena != nullptr)
            arenaDecay(&thread_state, arena, false, true);
        /// The missing arena case is not actually an error; a thread might be idle before it associates itself to
        /// one. This is unusual, but not wrong.
    }
    return 0;
}

/// jemalloc: tcache_create_ctl
int threadCacheCreate(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = readOnly(new_value, new_length))
        return result;
    if (int result = verifyRead<unsigned>(old_value, old_length_ptr))
        return result;
    unsigned thread_cache_idx;
    if (explicitThreadCachesCreate(thread_state, base0Get(), thread_cache_idx))
        return EFAULT;
    return read(old_value, old_length_ptr, thread_cache_idx);
}

/// No range check here (the callee handles it).
/// jemalloc: tcache_flush_ctl
int threadCacheFlush(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    unsigned thread_cache_idx;
    if (int result = assuredWrite(new_value, new_length, thread_cache_idx))
        return result;
    explicitThreadCachesFlush(thread_state, thread_cache_idx);
    return 0;
}

/// jemalloc: tcache_destroy_ctl
int threadCacheDestroy(
    ThreadState & thread_state, const size_t *, size_t, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (int result = writeOnly(old_value, old_length_ptr))
        return result;
    unsigned thread_cache_idx;
    if (int result = assuredWrite(new_value, new_length, thread_cache_idx))
        return result;
    explicitThreadCachesDestroy(thread_state, thread_cache_idx);
    return 0;
}

}
