/// The profiling "APIs" needed by other parts of the allocator, and the relevant "operational" data, mainly options
/// and mutexes; the core profiling data structures are encapsulated in ProfilingData.cpp (jemalloc: `prof.c`).

#include <allocator/Profiling.h>

#include <allocator/Base.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>
#include <allocator/PRNG.h>
#include <allocator/ProfilingHooks.h>
#include <allocator/ThreadEvent.h>

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <new>

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit bool profiling_active_state = false;
constinit std::atomic<bool> profiling_growth_dump_value{false};
constinit uint64_t profiling_interval = 0;
constinit size_t log2_profiling_sample = 0;
constinit bool profiling_booted = false;

namespace
{

/// Accessed via `profilingSampleEvent`. jemalloc: prof_idump_accumulated
constinit CounterAccumulated profiling_interval_dump_accumulated;

/// jemalloc: prof_active_mtx
constinit Mutex profiling_active_mutex;

/// Initialized as `opt.prof_thread_active_init`, and accessed via `profilingThreadActiveInit{Get,Set}`.
/// jemalloc: prof_thread_active_init, prof_thread_active_init_mtx
constinit bool profiling_thread_active_init = false;
constinit Mutex profiling_thread_active_init_mutex;

/// jemalloc: prof_gdump_mtx
constinit Mutex profiling_growth_dump_mutex;

/// jemalloc: next_thr_uid, next_thr_uid_mtx
constinit uint64_t next_thread_uid = 0;
constinit Mutex next_thread_uid_mutex;

/// jemalloc: prof_backtrace_hook, prof_dump_hook, prof_sample_hook, prof_sample_free_hook
constinit std::atomic<ProfilingBacktraceHook> profiling_backtrace_hook{nullptr};
constinit std::atomic<ProfilingDumpHook> profiling_dump_hook{nullptr};
constinit std::atomic<ProfilingSampleHook> profiling_sample_hook{nullptr};
constinit std::atomic<ProfilingSampleFreeHook> profiling_sample_free_hook{nullptr};

/// jemalloc: prof_active_assert
ALLOCATOR_ALWAYS_INLINE void profilingActiveAssert()
{
    /// If `opt.prof` is off, then `profiling_active` must always be off, regardless of whether `profiling_active_mutex` is in
    /// effect or not.
    ALLOCATOR_ASSERT(options.profiling || !profiling_active_state);
}

}

/// --- Sampled allocations -------------------------------------------------------------------------------------------

/// jemalloc: prof_alloc_rollback
void profilingAllocRollback(ThreadState & thread_state, ProfilingThreadContext * thread_context)
{
    if (thread_state.reentrancyLevel() > 0)
    {
        ALLOCATOR_ASSERT(thread_context == PROFILING_THREAD_CONTEXT_SENTINEL);
        return;
    }

    if (profilingThreadContextIsValid(thread_context))
    {
        thread_context->thread_data->lock->lock(&thread_state);
        thread_context->prepared = false;
        profilingThreadContextTryDestroy(thread_state, thread_context);
    }
}

/// jemalloc: prof_malloc_sample_object
void profilingMallocSampleObject(
    ThreadState & thread_state, const void * ptr, size_t size, size_t usable_size, ProfilingThreadContext * thread_context)
{
    if (options.profiling_system_thread_name)
        profilingSystemThreadNameFetch(thread_state);

    Extent * extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
    /// jemalloc: prof_info_set
    ALLOCATOR_ASSERT(extent != nullptr);
    ALLOCATOR_ASSERT(profilingThreadContextIsValid(thread_context));
    arenaProfilingInfoSet(thread_state, extent, thread_context, size);
    profilingFragmentationTrack(thread_state, extent, thread_context);

    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);

    thread_context->thread_data->lock->lock(&thread_state);
    /// We need to do these map lookups while holding the lock, to avoid the possibility of races with `prof.reset`
    /// calls, which update the map and then acquire the lock. This actually still leaves a data race on the contents
    /// of the unbias map; the key thing is to make sure that, if we read garbage data, the `prof.reset` call is about
    /// to mark our tctx as expired before any dumping of our corrupted output is attempted.
    size_t shifted_unbiased_count = profiling_shifted_unbiased_count[size_class_idx];
    size_t unbiased_bytes = profiling_unbiased_size[size_class_idx];
    ++thread_context->counts.current_objects;
    thread_context->counts.current_objects_shifted_unbiased += shifted_unbiased_count;
    thread_context->counts.current_bytes += usable_size;
    thread_context->counts.current_bytes_unbiased += unbiased_bytes;
    if (options.profiling_accumulated)
    {
        ++thread_context->counts.accumulated_objects;
        thread_context->counts.accumulated_objects_shifted_unbiased += shifted_unbiased_count;
        thread_context->counts.accumulated_bytes += usable_size;
        thread_context->counts.accumulated_bytes_unbiased += unbiased_bytes;
    }
    bool record_recent = profilingRecentAllocPrepare(thread_state, thread_context);
    thread_context->prepared = false;
    thread_context->thread_data->lock->unlock(&thread_state);
    if (record_recent)
    {
        ALLOCATOR_ASSERT(thread_context == extent->profilingThreadContext());
        profilingRecentAlloc(thread_state, extent, size, usable_size);
    }

    if (options.profiling_stats)
        profilingStatsIncrement(thread_state, size_class_idx, size);

    /// Sample hook.
    ProfilingSampleHook sample_hook = profilingSampleHookGet();
    if (sample_hook != nullptr)
    {
        ProfilingBacktrace * backtrace = &thread_context->global_context->backtrace;
        preReentrancy(thread_state, nullptr);
        sample_hook(ptr, size, backtrace->vector, backtrace->len, usable_size);
        postReentrancy(thread_state);
    }
}

/// jemalloc: prof_free_sampled_object
void profilingFreeSampledObject(ThreadState & thread_state, const void * ptr, size_t usable_size, ProfilingInfo * profiling_info)
{
    ALLOCATOR_ASSERT(profiling_info != nullptr);
    ProfilingThreadContext * thread_context = profiling_info->alloc_thread_context;
    ALLOCATOR_ASSERT(profilingThreadContextIsValid(thread_context));

    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);

    /// Unsample hook.
    ProfilingSampleFreeHook sample_free_hook = profilingSampleFreeHookGet();
    if (sample_free_hook != nullptr)
    {
        preReentrancy(thread_state, nullptr);
        sample_free_hook(ptr, usable_size);
        postReentrancy(thread_state);
    }

    thread_context->thread_data->lock->lock(&thread_state);

    ALLOCATOR_ASSERT(thread_context->counts.current_objects > 0);
    ALLOCATOR_ASSERT(thread_context->counts.current_bytes >= usable_size);
    /// It's not correct to do equivalent asserts for unbiased bytes, because of the potential for races with
    /// `prof.reset` calls.
    --thread_context->counts.current_objects;
    thread_context->counts.current_objects_shifted_unbiased -= profiling_shifted_unbiased_count[size_class_idx];
    thread_context->counts.current_bytes -= usable_size;
    thread_context->counts.current_bytes_unbiased -= profiling_unbiased_size[size_class_idx];

    /// `prof_try_log` is dropped together with `profiling_log`.

    profilingThreadContextTryDestroy(thread_state, thread_context);

    if (options.profiling_stats)
        profilingStatsDecrement(thread_state, size_class_idx, profiling_info->alloc_size);
}

/// jemalloc: prof_tctx_create
ProfilingThreadContext * profilingThreadContextCreate(ThreadState & thread_state)
{
    if (!thread_state.nominal() || thread_state.reentrancyLevel() > 0)
        return nullptr;

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return nullptr;

    ProfilingBacktrace backtrace;
    backtraceInit(&backtrace, thread_data->vector);
    profilingBacktrace(thread_state, &backtrace);
    return profilingLookup(thread_state, &backtrace);
}

/// The part of jemalloc's `prof_sample_should_skip` after the `sample_event` check.
bool profilingSampleShouldSkipSlow(ThreadState & thread_state)
{
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (ALLOCATOR_UNLIKELY(thread_data == nullptr))
        return true;
    return !thread_data->active;
}

/// --- The sampling event --------------------------------------------------------------------------------------------

/// jemalloc: prof_sample_new_event_wait
uint64_t profilingSampleNewEventWait(ThreadState & thread_state)
{
    if (log2_profiling_sample == 0)
        return THREAD_EVENT_MIN_START_WAIT;

    /// Compute sample interval as a geometrically distributed random variable with mean (2^lg_prof_sample):
    ///
    ///     bytes_until_sample = ceil(log(u) / log(1 - p)), where p = 1 / 2^lg_prof_sample
    ///
    /// (Luc Devroye, Non-Uniform Random Variate Generation, Springer-Verlag, New York, 1986, p. 500).
    ///
    /// In the actual computation, there's a non-zero probability that our pseudo random number generator generates
    /// an exact 0, and to avoid log(0), we set u to 1.0 in case r is 0. Therefore u effectively is uniformly
    /// distributed in (0, 1] instead of [0, 1). Further, rather than taking the ceiling, we take the floor and then
    /// add 1, since otherwise bytes_until_sample would be 0 if u is exactly 1.0.
    uint64_t r = prngLog2RangeU64(thread_state.prngState(), 53);
    double u = (r == 0U) ? 1.0 : double(static_cast<long double>(r) * (1.0L / 9007199254740992.0L));
    return uint64_t(log(u) / log(1.0 - (1.0 / double(uint64_t(1U) << log2_profiling_sample)))) + uint64_t(1U);
}

/// The postponed wait time for prof sample event is computed as if we want a new wait time (i.e. as if the event
/// were triggered). If we instead postpone to the immediate next allocation, like how we're handling the other
/// events, then we can have sampling bias, if e.g. the allocation immediately following a reentrancy always comes
/// from the same stack trace.
/// jemalloc: prof_sample_te_handler.postponed_event_wait = prof_sample_new_event_wait
uint64_t profilingSamplePostponedEventWait(ThreadState & thread_state)
{
    return profilingSampleNewEventWait(thread_state);
}

/// jemalloc: prof_sample_event_handler
void profilingSampleEvent(ThreadState & thread_state)
{
    if (profiling_interval == 0 || !profilingActiveGetUnlocked())
        return;
    uint64_t last_event = threadAllocatedLastEventGet(thread_state);
    uint64_t last_sample_event = profilingSampleLastEventGet(thread_state);
    profilingSampleLastEventSet(thread_state, last_event);
    uint64_t elapsed = last_event - last_sample_event;
    ALLOCATOR_ASSERT(elapsed > 0 && elapsed != THREAD_EVENT_INVALID_ELAPSED);
    if (profiling_interval_dump_accumulated.accumulate(&thread_state, elapsed))
        profilingIntervalDump(&thread_state);
}

/// --- Dumps ---------------------------------------------------------------------------------------------------------

namespace
{

/// The `atexit` callback of `profiling_final`. jemalloc: prof_fdump
void profilingFinalDump()
{
    ALLOCATOR_ASSERT(options.profiling_final);

    if (!profiling_booted)
        return;
    ThreadState & thread_state = ThreadState::fetch();
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    profilingFinalDumpImpl(thread_state);
}

/// jemalloc: prof_idump_accum_init
bool profilingIntervalDumpAccumulatedInit()
{
    return profiling_interval_dump_accumulated.init(profiling_interval);
}

}

/// jemalloc: prof_idump
void profilingIntervalDump(ThreadState * thread_state_ptr)
{
    if (!profiling_booted || thread_state_ptr == nullptr || !profilingActiveGetUnlocked())
        return;
    ThreadState & thread_state = *thread_state_ptr;
    if (thread_state.reentrancyLevel() > 0)
        return;

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return;
    if (thread_data->enqueued)
    {
        thread_data->enqueued_interval_dump = true;
        return;
    }

    profilingIntervalDumpImpl(thread_state);
}

/// jemalloc: prof_mdump
bool profilingManualDump(ThreadState & thread_state, const char * filename)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    if (!options.profiling || !profiling_booted)
        return true;

    return profilingManualDumpImpl(thread_state, filename);
}

/// jemalloc: prof_gdump
void profilingGrowthDump(ThreadState * thread_state_ptr)
{
    if (!profiling_booted || thread_state_ptr == nullptr || !profilingActiveGetUnlocked())
        return;
    ThreadState & thread_state = *thread_state_ptr;
    if (thread_state.reentrancyLevel() > 0)
        return;

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, false);
    if (thread_data == nullptr)
        return;
    if (thread_data->enqueued)
    {
        thread_data->enqueued_growth_dump = true;
        return;
    }

    profilingGrowthDumpImpl(thread_state);
}

/// --- Thread data ---------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_thr_uid_alloc
uint64_t profilingThreadUIDAlloc(ThreadState * thread_state)
{
    MutexLock lock(thread_state, next_thread_uid_mutex);
    uint64_t thread_uid = next_thread_uid;
    ++next_thread_uid;
    return thread_uid;
}

}

/// jemalloc: prof_tdata_init
ProfilingThreadData * profilingThreadDataInit(ThreadState & thread_state)
{
    return profilingThreadDataInitImpl(
        thread_state, profilingThreadUIDAlloc(&thread_state), 0, nullptr, profilingThreadActiveInitGet(&thread_state));
}

/// jemalloc: prof_tdata_reinit
ProfilingThreadData * profilingThreadDataReinit(ThreadState & thread_state, ProfilingThreadData * thread_data)
{
    uint64_t thread_uid = thread_data->thread_uid;
    uint64_t thread_discriminator = thread_data->thread_discriminator + 1;
    bool active = thread_data->active;

    /// Keep a local copy of the thread name, before detaching.
    profilingThreadNameAssert(thread_data);
    char thread_name[PROFILING_THREAD_NAME_MAX_LEN];
    strncpy(thread_name, thread_data->thread_name, PROFILING_THREAD_NAME_MAX_LEN);
    profilingThreadDataDetach(thread_state, thread_data);

    return profilingThreadDataInitImpl(thread_state, thread_uid, thread_discriminator, thread_name, active);
}

/// jemalloc: prof_tdata_cleanup
void profilingThreadDataCleanup(ThreadState & thread_state)
{
    ProfilingThreadData * thread_data = thread_state.profiling_thread_data;
    if (thread_data != nullptr)
        profilingThreadDataDetach(thread_state, thread_data);
}

/// jemalloc: prof_active_get
bool profilingActiveGet(ThreadState * thread_state)
{
    profilingActiveAssert();
    MutexLock lock(thread_state, profiling_active_mutex);
    return profiling_active_state;
}

/// jemalloc: prof_active_set
bool profilingActiveSet(ThreadState * thread_state, bool active)
{
    profilingActiveAssert();
    bool profiling_active_old;
    {
        MutexLock lock(thread_state, profiling_active_mutex);
        profiling_active_old = profiling_active_state;
        profiling_active_state = active;
    }
    profilingActiveAssert();
    return profiling_active_old;
}

/// jemalloc: prof_thread_name_get
const char * profilingThreadNameGet(ThreadState & thread_state)
{
    static const char * const profiling_thread_name_dummy = "";

    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);
    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return profiling_thread_name_dummy;

    return thread_data->thread_name;
}

/// jemalloc: prof_thread_name_set
int profilingThreadNameSet(ThreadState & thread_state, const char * thread_name)
{
    if (options.profiling_system_thread_name)
        return ENOENT;
    return profilingThreadNameSetImpl(thread_state, thread_name);
}

/// jemalloc: prof_thread_active_get
bool profilingThreadActiveGet(ThreadState & thread_state)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return false;
    return thread_data->active;
}

/// jemalloc: prof_thread_active_set
bool profilingThreadActiveSet(ThreadState & thread_state, bool active)
{
    ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);

    ProfilingThreadData * thread_data = profilingThreadDataGet(thread_state, true);
    if (thread_data == nullptr)
        return true;
    thread_data->active = active;
    return false;
}

/// jemalloc: prof_thread_active_init_get
bool profilingThreadActiveInitGet(ThreadState * thread_state)
{
    MutexLock lock(thread_state, profiling_thread_active_init_mutex);
    return profiling_thread_active_init;
}

/// jemalloc: prof_thread_active_init_set
bool profilingThreadActiveInitSet(ThreadState * thread_state, bool active_init)
{
    MutexLock lock(thread_state, profiling_thread_active_init_mutex);
    bool active_init_old = profiling_thread_active_init;
    profiling_thread_active_init = active_init;
    return active_init_old;
}

/// jemalloc: prof_gdump_get
bool profilingGrowthDumpGet(ThreadState * thread_state)
{
    MutexLock lock(thread_state, profiling_growth_dump_mutex);
    return profiling_growth_dump_value.load(std::memory_order_relaxed);
}

/// jemalloc: prof_gdump_set
bool profilingGrowthDumpSet(ThreadState * thread_state, bool growth_dump)
{
    MutexLock lock(thread_state, profiling_growth_dump_mutex);
    bool profiling_growth_dump_old = profiling_growth_dump_value.load(std::memory_order_relaxed);
    profiling_growth_dump_value.store(growth_dump, std::memory_order_relaxed);
    return profiling_growth_dump_old;
}

/// --- Hooks ---------------------------------------------------------------------------------------------------------

/// jemalloc: prof_backtrace_hook_set
void profilingBacktraceHookSet(ProfilingBacktraceHook hook)
{
    profiling_backtrace_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_backtrace_hook_get
ProfilingBacktraceHook profilingBacktraceHookGet()
{
    return profiling_backtrace_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_dump_hook_set
void profilingDumpHookSet(ProfilingDumpHook hook)
{
    profiling_dump_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_dump_hook_get
ProfilingDumpHook profilingDumpHookGet()
{
    return profiling_dump_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_sample_hook_set
void profilingSampleHookSet(ProfilingSampleHook hook)
{
    profiling_sample_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_sample_hook_get
ProfilingSampleHook profilingSampleHookGet()
{
    return profiling_sample_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_sample_free_hook_set
void profilingSampleFreeHookSet(ProfilingSampleFreeHook hook)
{
    profiling_sample_free_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_sample_free_hook_get
ProfilingSampleFreeHook profilingSampleFreeHookGet()
{
    return profiling_sample_free_hook.load(std::memory_order_acquire);
}

/// --- Boot ----------------------------------------------------------------------------------------------------------

/// jemalloc: prof_boot1
void profilingBoot1()
{
    /// `opt.prof` must be in its final state before any arenas are initialized, so this function must be executed
    /// early.
    if (options.profiling_leak_error && !options.profiling_leak)
        options.profiling_leak = true;

    if (options.profiling_leak && !options.profiling)
    {
        /// Enable `opt.prof`, but in such a way that profiles are never automatically dumped.
        options.profiling = true;
        options.profiling_growth_dump = false;
    }
    else if (options.profiling)
    {
        if (options.log2_profiling_interval >= 0)
            profiling_interval = uint64_t(1U) << options.log2_profiling_interval;
    }
}

/// jemalloc: prof_boot2
bool profilingBoot2(ThreadState & thread_state, Base * base)
{
    /// Initialize the global mutexes unconditionally to maintain correct stats when `opt.prof` is false.
    if (profiling_active_mutex.init("prof_active", MutexRank::PROFILING_ACTIVE))
        return true;
    if (profiling_growth_dump_mutex.init("prof_gdump", MutexRank::PROFILING_GROWTH_DUMP))
        return true;
    if (profiling_thread_active_init_mutex.init("prof_thread_active_init", MutexRank::PROFILING_THREAD_ACTIVE_INIT))
        return true;
    if (backtrace_to_global_context_mutex.init("prof_bt2gctx", MutexRank::PROFILING_BACKTRACE_TO_GLOBAL_CONTEXT))
        return true;
    if (all_thread_data_mutex.init("prof_tdatas", MutexRank::PROFILING_ALL_THREAD_DATA))
        return true;
    if (next_thread_uid_mutex.init("prof_next_thr_uid", MutexRank::PROFILING_NEXT_THREAD_UID))
        return true;
    if (profiling_stats_mutex.init("prof_stats", MutexRank::PROFILING_STATS))
        return true;
    if (profiling_dump_filename_mutex.init("prof_dump_filename", MutexRank::PROFILING_DUMP_FILENAME))
        return true;
    if (profiling_dump_mutex.init("prof_dump", MutexRank::PROFILING_DUMP))
        return true;

    if (options.profiling)
    {
        log2_profiling_sample = options.log2_profiling_sample;
        profilingUnbiasMapInit();
        profiling_active_state = options.profiling_active;
        profiling_growth_dump_value.store(options.profiling_growth_dump, std::memory_order_relaxed);
        profiling_thread_active_init = options.profiling_thread_active_init;

        if (profilingDataInit(thread_state))
            return true;

        next_thread_uid = 0;
        if (profilingIntervalDumpAccumulatedInit())
            return true;

        if (options.profiling_final && options.profiling_prefix[0] != '\0' && atexit(profilingFinalDump) != 0)
        {
            writeMessage("<jemalloc>: Error in atexit()\n");
            if (options.abort)
                abort();
        }

        /// `prof_log_init` is dropped together with `profiling_log`.

        if (profilingRecentInit())
            return true;

        profiling_base = base;

        global_context_locks = static_cast<Mutex *>(base->alloc(&thread_state, PROFILING_NUM_CONTEXT_LOCKS * sizeof(Mutex), CACHE_LINE));
        if (global_context_locks == nullptr)
            return true;
        for (unsigned i = 0; i < PROFILING_NUM_CONTEXT_LOCKS; ++i)
        {
            Mutex * mutex = new (&global_context_locks[i]) Mutex;
            if (mutex->init("prof_gctx", MutexRank::PROFILING_GLOBAL_CONTEXT))
                return true;
        }

        thread_data_locks = static_cast<Mutex *>(base->alloc(&thread_state, PROFILING_NUM_THREAD_DATA_LOCKS * sizeof(Mutex), CACHE_LINE));
        if (thread_data_locks == nullptr)
            return true;
        for (unsigned i = 0; i < PROFILING_NUM_THREAD_DATA_LOCKS; ++i)
        {
            Mutex * mutex = new (&thread_data_locks[i]) Mutex;
            if (mutex->init("prof_tdata", MutexRank::PROFILING_THREAD_DATA))
                return true;
        }

        profilingUnwindInit();
        profilingHooksInit();
    }
    profiling_booted = true;

    return false;
}

/// --- Fork ----------------------------------------------------------------------------------------------------------

/// jemalloc: prof_prefork0 (`log_mtx` is dropped together with `prof_log`)
void profilingPrefork0(ThreadState * thread_state)
{
    if (config::profiling && options.profiling)
    {
        profiling_dump_mutex.prefork(thread_state);
        backtrace_to_global_context_mutex.prefork(thread_state);
        all_thread_data_mutex.prefork(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_THREAD_DATA_LOCKS; ++i)
            thread_data_locks[i].prefork(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_CONTEXT_LOCKS; ++i)
            global_context_locks[i].prefork(thread_state);
        profiling_recent_dump_mutex.prefork(thread_state);
    }
}

/// jemalloc: prof_prefork1
void profilingPrefork1(ThreadState * thread_state)
{
    if (config::profiling && options.profiling)
    {
        profiling_interval_dump_accumulated.prefork(thread_state);
        profiling_active_mutex.prefork(thread_state);
        profiling_dump_filename_mutex.prefork(thread_state);
        profiling_growth_dump_mutex.prefork(thread_state);
        profiling_recent_alloc_mutex.prefork(thread_state);
        profiling_stats_mutex.prefork(thread_state);
        next_thread_uid_mutex.prefork(thread_state);
        profiling_thread_active_init_mutex.prefork(thread_state);
    }
}

/// jemalloc: prof_postfork_parent
void profilingPostforkParent(ThreadState * thread_state)
{
    if (config::profiling && options.profiling)
    {
        profiling_thread_active_init_mutex.postforkParent(thread_state);
        next_thread_uid_mutex.postforkParent(thread_state);
        profiling_stats_mutex.postforkParent(thread_state);
        profiling_recent_alloc_mutex.postforkParent(thread_state);
        profiling_growth_dump_mutex.postforkParent(thread_state);
        profiling_dump_filename_mutex.postforkParent(thread_state);
        profiling_active_mutex.postforkParent(thread_state);
        profiling_interval_dump_accumulated.postforkParent(thread_state);
        profiling_recent_dump_mutex.postforkParent(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_CONTEXT_LOCKS; ++i)
            global_context_locks[i].postforkParent(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_THREAD_DATA_LOCKS; ++i)
            thread_data_locks[i].postforkParent(thread_state);
        all_thread_data_mutex.postforkParent(thread_state);
        backtrace_to_global_context_mutex.postforkParent(thread_state);
        profiling_dump_mutex.postforkParent(thread_state);
    }
}

/// jemalloc: prof_postfork_child
void profilingPostforkChild(ThreadState * thread_state)
{
    if (config::profiling && options.profiling)
    {
        profiling_thread_active_init_mutex.postforkChild(thread_state);
        next_thread_uid_mutex.postforkChild(thread_state);
        profiling_stats_mutex.postforkChild(thread_state);
        profiling_recent_alloc_mutex.postforkChild(thread_state);
        profiling_growth_dump_mutex.postforkChild(thread_state);
        profiling_dump_filename_mutex.postforkChild(thread_state);
        profiling_active_mutex.postforkChild(thread_state);
        profiling_interval_dump_accumulated.postforkChild(thread_state);
        profiling_recent_dump_mutex.postforkChild(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_CONTEXT_LOCKS; ++i)
            global_context_locks[i].postforkChild(thread_state);
        for (unsigned i = 0; i < PROFILING_NUM_THREAD_DATA_LOCKS; ++i)
            thread_data_locks[i].postforkChild(thread_state);
        all_thread_data_mutex.postforkChild(thread_state);
        backtrace_to_global_context_mutex.postforkChild(thread_state);
        profiling_dump_mutex.postforkChild(thread_state);
    }
}

}
