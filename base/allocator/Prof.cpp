/// The profiling "APIs" needed by other parts of the allocator, and the relevant "operational" data, mainly options
/// and mutexes; the core profiling data structures are encapsulated in ProfData.cpp (jemalloc: `prof.c`).

#include <allocator/Prof.h>

#include <allocator/Base.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Options.h>
#include <allocator/Prng.h>
#include <allocator/ProfHooks.h>
#include <allocator/ThreadEvent.h>

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <new>

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit bool prof_active_state = false;
constinit std::atomic<bool> prof_gdump_val{false};
constinit uint64_t prof_interval = 0;
constinit size_t lg_prof_sample = 0;
constinit bool prof_booted = false;

namespace
{

/// Accessed via `profSampleEvent`. jemalloc: prof_idump_accumulated
constinit CounterAccum prof_idump_accumulated;

/// jemalloc: prof_active_mtx
constinit Mutex prof_active_mtx;

/// Initialized as `opt.prof_thread_active_init`, and accessed via `profThreadActiveInit{Get,Set}`.
/// jemalloc: prof_thread_active_init, prof_thread_active_init_mtx
constinit bool prof_thread_active_init = false;
constinit Mutex prof_thread_active_init_mtx;

/// jemalloc: prof_gdump_mtx
constinit Mutex prof_gdump_mtx;

/// jemalloc: next_thr_uid, next_thr_uid_mtx
constinit uint64_t next_thr_uid = 0;
constinit Mutex next_thr_uid_mtx;

/// jemalloc: prof_backtrace_hook, prof_dump_hook, prof_sample_hook, prof_sample_free_hook
constinit std::atomic<ProfBacktraceHook> prof_backtrace_hook{nullptr};
constinit std::atomic<ProfDumpHook> prof_dump_hook{nullptr};
constinit std::atomic<ProfSampleHook> prof_sample_hook{nullptr};
constinit std::atomic<ProfSampleFreeHook> prof_sample_free_hook{nullptr};

/// jemalloc: prof_active_assert
JE_ALWAYS_INLINE void profActiveAssert()
{
    /// If `opt.prof` is off, then `prof_active` must always be off, regardless of whether `prof_active_mtx` is in
    /// effect or not.
    JE_ASSERT(opt.prof || !prof_active_state);
}

}

/// --- Sampled allocations -------------------------------------------------------------------------------------------

/// jemalloc: prof_alloc_rollback
void profAllocRollback(ThreadState & tsd, ProfThreadContext * tctx)
{
    if (tsd.reentrancyLevel() > 0)
    {
        JE_ASSERT(tctx == PROF_TCTX_SENTINEL);
        return;
    }

    if (profTctxIsValid(tctx))
    {
        tctx->tdata->lock->lock(&tsd);
        tctx->prepared = false;
        profTctxTryDestroy(tsd, tctx);
    }
}

/// jemalloc: prof_malloc_sample_object
void profMallocSampleObject(ThreadState & tsd, const void * ptr, size_t size, size_t usize, ProfThreadContext * tctx)
{
    if (opt.prof_sys_thread_name)
        profSysThreadNameFetch(tsd);

    Extent * edata = arena_emap_global.edataLookup(&tsd, ptr);
    /// jemalloc: prof_info_set
    JE_ASSERT(edata != nullptr);
    JE_ASSERT(profTctxIsValid(tctx));
    arenaProfInfoSet(tsd, edata, tctx, size);
    profFragTrack(tsd, edata, tctx);

    szind_t szind = sz::sizeToIndex(usize);

    tctx->tdata->lock->lock(&tsd);
    /// We need to do these map lookups while holding the lock, to avoid the possibility of races with `prof.reset`
    /// calls, which update the map and then acquire the lock. This actually still leaves a data race on the contents
    /// of the unbias map; the key thing is to make sure that, if we read garbage data, the `prof.reset` call is about
    /// to mark our tctx as expired before any dumping of our corrupted output is attempted.
    size_t shifted_unbiased_cnt = prof_shifted_unbiased_cnt[szind];
    size_t unbiased_bytes = prof_unbiased_sz[szind];
    ++tctx->cnts.curobjs;
    tctx->cnts.curobjs_shifted_unbiased += shifted_unbiased_cnt;
    tctx->cnts.curbytes += usize;
    tctx->cnts.curbytes_unbiased += unbiased_bytes;
    if (opt.prof_accum)
    {
        ++tctx->cnts.accumobjs;
        tctx->cnts.accumobjs_shifted_unbiased += shifted_unbiased_cnt;
        tctx->cnts.accumbytes += usize;
        tctx->cnts.accumbytes_unbiased += unbiased_bytes;
    }
    bool record_recent = profRecentAllocPrepare(tsd, tctx);
    tctx->prepared = false;
    tctx->tdata->lock->unlock(&tsd);
    if (record_recent)
    {
        JE_ASSERT(tctx == edata->profTctx());
        profRecentAlloc(tsd, edata, size, usize);
    }

    if (opt.prof_stats)
        profStatsInc(tsd, szind, size);

    /// Sample hook.
    ProfSampleHook sample_hook = profSampleHookGet();
    if (sample_hook != nullptr)
    {
        ProfBacktrace * bt = &tctx->gctx->bt;
        preReentrancy(tsd, nullptr);
        sample_hook(ptr, size, bt->vec, bt->len, usize);
        postReentrancy(tsd);
    }
}

/// jemalloc: prof_free_sampled_object
void profFreeSampledObject(ThreadState & tsd, const void * ptr, size_t usize, ProfInfo * prof_info)
{
    JE_ASSERT(prof_info != nullptr);
    ProfThreadContext * tctx = prof_info->alloc_tctx;
    JE_ASSERT(profTctxIsValid(tctx));

    szind_t szind = sz::sizeToIndex(usize);

    /// Unsample hook.
    ProfSampleFreeHook sample_free_hook = profSampleFreeHookGet();
    if (sample_free_hook != nullptr)
    {
        preReentrancy(tsd, nullptr);
        sample_free_hook(ptr, usize);
        postReentrancy(tsd);
    }

    tctx->tdata->lock->lock(&tsd);

    JE_ASSERT(tctx->cnts.curobjs > 0);
    JE_ASSERT(tctx->cnts.curbytes >= usize);
    /// It's not correct to do equivalent asserts for unbiased bytes, because of the potential for races with
    /// `prof.reset` calls.
    --tctx->cnts.curobjs;
    tctx->cnts.curobjs_shifted_unbiased -= prof_shifted_unbiased_cnt[szind];
    tctx->cnts.curbytes -= usize;
    tctx->cnts.curbytes_unbiased -= prof_unbiased_sz[szind];

    /// `prof_try_log` is dropped together with `prof_log`.

    profTctxTryDestroy(tsd, tctx);

    if (opt.prof_stats)
        profStatsDec(tsd, szind, prof_info->alloc_size);
}

/// jemalloc: prof_tctx_create
ProfThreadContext * profTctxCreate(ThreadState & tsd)
{
    if (!tsd.nominal() || tsd.reentrancyLevel() > 0)
        return nullptr;

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return nullptr;

    ProfBacktrace bt;
    btInit(&bt, tdata->vec);
    profBacktrace(tsd, &bt);
    return profLookup(tsd, &bt);
}

/// The part of jemalloc's `prof_sample_should_skip` after the `sample_event` check.
bool profSampleShouldSkipSlow(ThreadState & tsd)
{
    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (JE_UNLIKELY(tdata == nullptr))
        return true;
    return !tdata->active;
}

/// --- The sampling event --------------------------------------------------------------------------------------------

/// jemalloc: prof_sample_new_event_wait
uint64_t profSampleNewEventWait(ThreadState & tsd)
{
    if (lg_prof_sample == 0)
        return TE_MIN_START_WAIT;

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
    uint64_t r = prngLgRangeU64(tsd.prngState(), 53);
    double u = (r == 0U) ? 1.0 : double(static_cast<long double>(r) * (1.0L / 9007199254740992.0L));
    return uint64_t(log(u) / log(1.0 - (1.0 / double(uint64_t(1U) << lg_prof_sample)))) + uint64_t(1U);
}

/// The postponed wait time for prof sample event is computed as if we want a new wait time (i.e. as if the event
/// were triggered). If we instead postpone to the immediate next allocation, like how we're handling the other
/// events, then we can have sampling bias, if e.g. the allocation immediately following a reentrancy always comes
/// from the same stack trace.
/// jemalloc: prof_sample_te_handler.postponed_event_wait = prof_sample_new_event_wait
uint64_t profSamplePostponedEventWait(ThreadState & tsd)
{
    return profSampleNewEventWait(tsd);
}

/// jemalloc: prof_sample_event_handler
void profSampleEvent(ThreadState & tsd)
{
    if (prof_interval == 0 || !profActiveGetUnlocked())
        return;
    uint64_t last_event = threadAllocatedLastEventGet(tsd);
    uint64_t last_sample_event = profSampleLastEventGet(tsd);
    profSampleLastEventSet(tsd, last_event);
    uint64_t elapsed = last_event - last_sample_event;
    JE_ASSERT(elapsed > 0 && elapsed != TE_INVALID_ELAPSED);
    if (prof_idump_accumulated.accum(&tsd, elapsed))
        profIdump(&tsd);
}

/// --- Dumps ---------------------------------------------------------------------------------------------------------

namespace
{

/// The `atexit` callback of `prof_final`. jemalloc: prof_fdump
void profFdump()
{
    JE_ASSERT(opt.prof_final);

    if (!prof_booted)
        return;
    ThreadState & tsd = ThreadState::fetch();
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    profFdumpImpl(tsd);
}

/// jemalloc: prof_idump_accum_init
bool profIdumpAccumInit()
{
    return prof_idump_accumulated.init(prof_interval);
}

}

/// jemalloc: prof_idump
void profIdump(ThreadState * tsdn)
{
    if (!prof_booted || tsdn == nullptr || !profActiveGetUnlocked())
        return;
    ThreadState & tsd = *tsdn;
    if (tsd.reentrancyLevel() > 0)
        return;

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return;
    if (tdata->enq)
    {
        tdata->enq_idump = true;
        return;
    }

    profIdumpImpl(tsd);
}

/// jemalloc: prof_mdump
bool profMdump(ThreadState & tsd, const char * filename)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    if (!opt.prof || !prof_booted)
        return true;

    return profMdumpImpl(tsd, filename);
}

/// jemalloc: prof_gdump
void profGdump(ThreadState * tsdn)
{
    if (!prof_booted || tsdn == nullptr || !profActiveGetUnlocked())
        return;
    ThreadState & tsd = *tsdn;
    if (tsd.reentrancyLevel() > 0)
        return;

    ProfThreadData * tdata = profTdataGet(tsd, false);
    if (tdata == nullptr)
        return;
    if (tdata->enq)
    {
        tdata->enq_gdump = true;
        return;
    }

    profGdumpImpl(tsd);
}

/// --- Thread data ---------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: prof_thr_uid_alloc
uint64_t profThrUidAlloc(ThreadState * tsdn)
{
    MutexLock lock(tsdn, next_thr_uid_mtx);
    uint64_t thr_uid = next_thr_uid;
    ++next_thr_uid;
    return thr_uid;
}

}

/// jemalloc: prof_tdata_init
ProfThreadData * profTdataInit(ThreadState & tsd)
{
    return profTdataInitImpl(tsd, profThrUidAlloc(&tsd), 0, nullptr, profThreadActiveInitGet(&tsd));
}

/// jemalloc: prof_tdata_reinit
ProfThreadData * profTdataReinit(ThreadState & tsd, ProfThreadData * tdata)
{
    uint64_t thr_uid = tdata->thr_uid;
    uint64_t thr_discrim = tdata->thr_discrim + 1;
    bool active = tdata->active;

    /// Keep a local copy of the thread name, before detaching.
    profThreadNameAssert(tdata);
    char thread_name[PROF_THREAD_NAME_MAX_LEN];
    strncpy(thread_name, tdata->thread_name, PROF_THREAD_NAME_MAX_LEN);
    profTdataDetach(tsd, tdata);

    return profTdataInitImpl(tsd, thr_uid, thr_discrim, thread_name, active);
}

/// jemalloc: prof_tdata_cleanup
void profTdataCleanup(ThreadState & tsd)
{
    ProfThreadData * tdata = tsd.prof_tdata;
    if (tdata != nullptr)
        profTdataDetach(tsd, tdata);
}

/// jemalloc: prof_active_get
bool profActiveGet(ThreadState * tsdn)
{
    profActiveAssert();
    MutexLock lock(tsdn, prof_active_mtx);
    return prof_active_state;
}

/// jemalloc: prof_active_set
bool profActiveSet(ThreadState * tsdn, bool active)
{
    profActiveAssert();
    bool prof_active_old;
    {
        MutexLock lock(tsdn, prof_active_mtx);
        prof_active_old = prof_active_state;
        prof_active_state = active;
    }
    profActiveAssert();
    return prof_active_old;
}

/// jemalloc: prof_thread_name_get
const char * profThreadNameGet(ThreadState & tsd)
{
    static const char * const prof_thread_name_dummy = "";

    JE_ASSERT(tsd.reentrancyLevel() == 0);
    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return prof_thread_name_dummy;

    return tdata->thread_name;
}

/// jemalloc: prof_thread_name_set
int profThreadNameSet(ThreadState & tsd, const char * thread_name)
{
    if (opt.prof_sys_thread_name)
        return ENOENT;
    return profThreadNameSetImpl(tsd, thread_name);
}

/// jemalloc: prof_thread_active_get
bool profThreadActiveGet(ThreadState & tsd)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return false;
    return tdata->active;
}

/// jemalloc: prof_thread_active_set
bool profThreadActiveSet(ThreadState & tsd, bool active)
{
    JE_ASSERT(tsd.reentrancyLevel() == 0);

    ProfThreadData * tdata = profTdataGet(tsd, true);
    if (tdata == nullptr)
        return true;
    tdata->active = active;
    return false;
}

/// jemalloc: prof_thread_active_init_get
bool profThreadActiveInitGet(ThreadState * tsdn)
{
    MutexLock lock(tsdn, prof_thread_active_init_mtx);
    return prof_thread_active_init;
}

/// jemalloc: prof_thread_active_init_set
bool profThreadActiveInitSet(ThreadState * tsdn, bool active_init)
{
    MutexLock lock(tsdn, prof_thread_active_init_mtx);
    bool active_init_old = prof_thread_active_init;
    prof_thread_active_init = active_init;
    return active_init_old;
}

/// jemalloc: prof_gdump_get
bool profGdumpGet(ThreadState * tsdn)
{
    MutexLock lock(tsdn, prof_gdump_mtx);
    return prof_gdump_val.load(std::memory_order_relaxed);
}

/// jemalloc: prof_gdump_set
bool profGdumpSet(ThreadState * tsdn, bool gdump)
{
    MutexLock lock(tsdn, prof_gdump_mtx);
    bool prof_gdump_old = prof_gdump_val.load(std::memory_order_relaxed);
    prof_gdump_val.store(gdump, std::memory_order_relaxed);
    return prof_gdump_old;
}

/// --- Hooks ---------------------------------------------------------------------------------------------------------

/// jemalloc: prof_backtrace_hook_set
void profBacktraceHookSet(ProfBacktraceHook hook)
{
    prof_backtrace_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_backtrace_hook_get
ProfBacktraceHook profBacktraceHookGet()
{
    return prof_backtrace_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_dump_hook_set
void profDumpHookSet(ProfDumpHook hook)
{
    prof_dump_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_dump_hook_get
ProfDumpHook profDumpHookGet()
{
    return prof_dump_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_sample_hook_set
void profSampleHookSet(ProfSampleHook hook)
{
    prof_sample_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_sample_hook_get
ProfSampleHook profSampleHookGet()
{
    return prof_sample_hook.load(std::memory_order_acquire);
}

/// jemalloc: prof_sample_free_hook_set
void profSampleFreeHookSet(ProfSampleFreeHook hook)
{
    prof_sample_free_hook.store(hook, std::memory_order_release);
}

/// jemalloc: prof_sample_free_hook_get
ProfSampleFreeHook profSampleFreeHookGet()
{
    return prof_sample_free_hook.load(std::memory_order_acquire);
}

/// --- Boot ----------------------------------------------------------------------------------------------------------

/// jemalloc: prof_boot1
void profBoot1()
{
    /// `opt.prof` must be in its final state before any arenas are initialized, so this function must be executed
    /// early.
    if (opt.prof_leak_error && !opt.prof_leak)
        opt.prof_leak = true;

    if (opt.prof_leak && !opt.prof)
    {
        /// Enable `opt.prof`, but in such a way that profiles are never automatically dumped.
        opt.prof = true;
        opt.prof_gdump = false;
    }
    else if (opt.prof)
    {
        if (opt.lg_prof_interval >= 0)
            prof_interval = uint64_t(1U) << opt.lg_prof_interval;
    }
}

/// jemalloc: prof_boot2
bool profBoot2(ThreadState & tsd, Base * base)
{
    /// Initialize the global mutexes unconditionally to maintain correct stats when `opt.prof` is false.
    if (prof_active_mtx.init("prof_active", MutexRank::PROF_ACTIVE))
        return true;
    if (prof_gdump_mtx.init("prof_gdump", MutexRank::PROF_GDUMP))
        return true;
    if (prof_thread_active_init_mtx.init("prof_thread_active_init", MutexRank::PROF_THREAD_ACTIVE_INIT))
        return true;
    if (bt2gctx_mtx.init("prof_bt2gctx", MutexRank::PROF_BT2GCTX))
        return true;
    if (tdatas_mtx.init("prof_tdatas", MutexRank::PROF_TDATAS))
        return true;
    if (next_thr_uid_mtx.init("prof_next_thr_uid", MutexRank::PROF_NEXT_THR_UID))
        return true;
    if (prof_stats_mtx.init("prof_stats", MutexRank::PROF_STATS))
        return true;
    if (prof_dump_filename_mtx.init("prof_dump_filename", MutexRank::PROF_DUMP_FILENAME))
        return true;
    if (prof_dump_mtx.init("prof_dump", MutexRank::PROF_DUMP))
        return true;

    if (opt.prof)
    {
        lg_prof_sample = opt.lg_prof_sample;
        profUnbiasMapInit();
        prof_active_state = opt.prof_active;
        prof_gdump_val.store(opt.prof_gdump, std::memory_order_relaxed);
        prof_thread_active_init = opt.prof_thread_active_init;

        if (profDataInit(tsd))
            return true;

        next_thr_uid = 0;
        if (profIdumpAccumInit())
            return true;

        if (opt.prof_final && opt.prof_prefix[0] != '\0' && atexit(profFdump) != 0)
        {
            writeMessage("<jemalloc>: Error in atexit()\n");
            if (opt.abort)
                abort();
        }

        /// `prof_log_init` is dropped together with `prof_log`.

        if (profRecentInit())
            return true;

        prof_base = base;

        gctx_locks = static_cast<Mutex *>(base->alloc(&tsd, PROF_NCTX_LOCKS * sizeof(Mutex), CACHELINE));
        if (gctx_locks == nullptr)
            return true;
        for (unsigned i = 0; i < PROF_NCTX_LOCKS; ++i)
        {
            Mutex * mutex = new (&gctx_locks[i]) Mutex;
            if (mutex->init("prof_gctx", MutexRank::PROF_GCTX))
                return true;
        }

        tdata_locks = static_cast<Mutex *>(base->alloc(&tsd, PROF_NTDATA_LOCKS * sizeof(Mutex), CACHELINE));
        if (tdata_locks == nullptr)
            return true;
        for (unsigned i = 0; i < PROF_NTDATA_LOCKS; ++i)
        {
            Mutex * mutex = new (&tdata_locks[i]) Mutex;
            if (mutex->init("prof_tdata", MutexRank::PROF_TDATA))
                return true;
        }

        profUnwindInit();
        profHooksInit();
    }
    prof_booted = true;

    return false;
}

/// --- Fork ----------------------------------------------------------------------------------------------------------

/// jemalloc: prof_prefork0 (`log_mtx` is dropped together with `prof_log`)
void profPrefork0(ThreadState * tsdn)
{
    if (config::prof && opt.prof)
    {
        prof_dump_mtx.prefork(tsdn);
        bt2gctx_mtx.prefork(tsdn);
        tdatas_mtx.prefork(tsdn);
        for (unsigned i = 0; i < PROF_NTDATA_LOCKS; ++i)
            tdata_locks[i].prefork(tsdn);
        for (unsigned i = 0; i < PROF_NCTX_LOCKS; ++i)
            gctx_locks[i].prefork(tsdn);
        prof_recent_dump_mtx.prefork(tsdn);
    }
}

/// jemalloc: prof_prefork1
void profPrefork1(ThreadState * tsdn)
{
    if (config::prof && opt.prof)
    {
        prof_idump_accumulated.prefork(tsdn);
        prof_active_mtx.prefork(tsdn);
        prof_dump_filename_mtx.prefork(tsdn);
        prof_gdump_mtx.prefork(tsdn);
        prof_recent_alloc_mtx.prefork(tsdn);
        prof_stats_mtx.prefork(tsdn);
        next_thr_uid_mtx.prefork(tsdn);
        prof_thread_active_init_mtx.prefork(tsdn);
    }
}

/// jemalloc: prof_postfork_parent
void profPostforkParent(ThreadState * tsdn)
{
    if (config::prof && opt.prof)
    {
        prof_thread_active_init_mtx.postforkParent(tsdn);
        next_thr_uid_mtx.postforkParent(tsdn);
        prof_stats_mtx.postforkParent(tsdn);
        prof_recent_alloc_mtx.postforkParent(tsdn);
        prof_gdump_mtx.postforkParent(tsdn);
        prof_dump_filename_mtx.postforkParent(tsdn);
        prof_active_mtx.postforkParent(tsdn);
        prof_idump_accumulated.postforkParent(tsdn);
        prof_recent_dump_mtx.postforkParent(tsdn);
        for (unsigned i = 0; i < PROF_NCTX_LOCKS; ++i)
            gctx_locks[i].postforkParent(tsdn);
        for (unsigned i = 0; i < PROF_NTDATA_LOCKS; ++i)
            tdata_locks[i].postforkParent(tsdn);
        tdatas_mtx.postforkParent(tsdn);
        bt2gctx_mtx.postforkParent(tsdn);
        prof_dump_mtx.postforkParent(tsdn);
    }
}

/// jemalloc: prof_postfork_child
void profPostforkChild(ThreadState * tsdn)
{
    if (config::prof && opt.prof)
    {
        prof_thread_active_init_mtx.postforkChild(tsdn);
        next_thr_uid_mtx.postforkChild(tsdn);
        prof_stats_mtx.postforkChild(tsdn);
        prof_recent_alloc_mtx.postforkChild(tsdn);
        prof_gdump_mtx.postforkChild(tsdn);
        prof_dump_filename_mtx.postforkChild(tsdn);
        prof_active_mtx.postforkChild(tsdn);
        prof_idump_accumulated.postforkChild(tsdn);
        prof_recent_dump_mtx.postforkChild(tsdn);
        for (unsigned i = 0; i < PROF_NCTX_LOCKS; ++i)
            gctx_locks[i].postforkChild(tsdn);
        for (unsigned i = 0; i < PROF_NTDATA_LOCKS; ++i)
            tdata_locks[i].postforkChild(tsdn);
        tdatas_mtx.postforkChild(tsdn);
        bt2gctx_mtx.postforkChild(tsdn);
        prof_dump_mtx.postforkChild(tsdn);
    }
}

}
