#include <allocator/BackgroundThread.h>

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/Base.h>
#include <allocator/Format.h>
#include <allocator/ThreadState.h>

#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <dlfcn.h>
#include <new>
#include <pthread.h>
#include <sched.h>

#if defined(__FreeBSD__)
#    include <pthread_np.h>
#endif

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit Mutex background_thread_lock;
constinit std::atomic<bool> background_thread_enabled_state{false};
constinit size_t n_background_threads = 0;
constinit size_t max_background_threads = 0;
constinit BackgroundThreadInfo * background_thread_info = nullptr;

namespace
{

/// jemalloc: background_thread_enabled_at_fork
constinit bool background_thread_enabled_at_fork = false;

using PthreadCreateFunction = int (*)(pthread_t *, const pthread_attr_t *, void * (*)(void *), void *);

/// jemalloc: pthread_create_fptr
constinit PthreadCreateFunction pthread_create_fptr = nullptr;

/// jemalloc: pthread_create_wrapper_init
void pthreadCreateWrapperInit()
{
    if constexpr (config::lazy_lock)
    {
        if (!isthreaded)
            isthreaded = true;
    }
}

/// Returns true on error (never: a failed lookup aborts or falls back).
/// jemalloc: pthread_create_fptr_init
bool pthreadCreateFptrInit()
{
    if (pthread_create_fptr != nullptr)
        return false;
    /// Try the next symbol first, because 1) when use lazy_lock we have a wrapper for pthread_create; and 2)
    /// application may define its own wrapper as well (and can call malloc within the wrapper).
    pthread_create_fptr = reinterpret_cast<PthreadCreateFunction>(dlsym(RTLD_NEXT, "pthread_create"));
    if (pthread_create_fptr == nullptr)
        pthread_create_fptr = reinterpret_cast<PthreadCreateFunction>(dlsym(RTLD_DEFAULT, "pthread_create"));
    if (pthread_create_fptr == nullptr)
    {
        if constexpr (config::lazy_lock)
        {
            writeMessage("<jemalloc>: Error in dlsym(RTLD_NEXT, \"pthread_create\")\n");
            abort();
        }
        else
        {
            /// Fall back to the default symbol.
            pthread_create_fptr = pthread_create;
        }
    }

    return false;
}

/// Initializes the condition variable of a thread info with the clock of `NsTime` (`CLOCK_MONOTONIC` where
/// available). At boot any failure is an error; after fork (`fallback_to_default`) the default attributes
/// (`CLOCK_REALTIME`) are used if the clock cannot be set (fork patch 861db0b4). Returns the error of
/// `pthread_cond_init` (non-zero also for an attribute failure at boot).
/// jemalloc: the condition variable initialization in background_thread_boot1 and background_thread_postfork_child
int condInit(pthread_cond_t * cond, bool fallback_to_default)
{
#if !defined(__APPLE__)
    static_assert(config::have_clock_monotonic);
    pthread_condattr_t cond_attr;
    if (pthread_condattr_init(&cond_attr))
        return fallback_to_default ? pthread_cond_init(cond, nullptr) : 1;
    if (pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC))
    {
        /// Fall back to default (CLOCK_REALTIME) attributes if setclock fails.
        pthread_condattr_destroy(&cond_attr);
        return fallback_to_default ? pthread_cond_init(cond, nullptr) : 1;
    }
    int ret = pthread_cond_init(cond, &cond_attr);
    pthread_condattr_destroy(&cond_attr);
    return ret;
#else
    static_assert(!config::have_clock_monotonic);
    (void)fallback_to_default;
    return pthread_cond_init(cond, nullptr);
#endif
}

/// jemalloc: background_thread_info_init
void backgroundThreadInfoInit(ThreadState * tsdn, BackgroundThreadInfo * info)
{
    info->wakeupTimeSet(tsdn, 0);
    info->npages_to_purge_new = 0;
    if constexpr (config::stats)
    {
        info->tot_n_runs = 0;
        info->tot_sleep_time.initZero();
    }
}

/// Returns true on error (the result is ignored by the caller).
/// jemalloc: set_current_thread_affinity
bool setCurrentThreadAffinity(int cpu)
{
#if defined(__linux__) || (defined(__FreeBSD__) && defined(__powerpc64__))
    static_assert(config::have_sched_setaffinity);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    return sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0;
#else
    static_assert(!config::have_sched_setaffinity);
    (void)cpu;
    return false;
#endif
}

/// jemalloc: the `pthread_setname_np` call in background_thread_entry (JEMALLOC_HAVE_PTHREAD_SETNAME_NP)
void setCurrentThreadName()
{
#if defined(__linux__) || (defined(__FreeBSD__) && defined(__powerpc64__))
    static_assert(config::have_pthread_setname_np);
    pthread_setname_np(pthread_self(), "jemalloc_bg_thd");
#else
    static_assert(!config::have_pthread_setname_np);
#endif
}

/// `pthread_cond_wait` drops and re-acquires the mutex internally, without going through our wrapper. Update the
/// locked state explicitly.
/// jemalloc: background_thread_cond_wait
int backgroundThreadCondWait(BackgroundThreadInfo * info, const struct timespec * ts)
{
    int ret;

    info->mtx.setLockedFlag(false);
    if (ts == nullptr)
        ret = pthread_cond_wait(&info->cond, info->mtx.nativeHandle());
    else
        ret = pthread_cond_timedwait(&info->cond, info->mtx.nativeHandle(), ts);
    info->mtx.setLockedFlag(true);

    return ret;
}

/// jemalloc: background_thread_sleep
void backgroundThreadSleep(ThreadState * tsdn, BackgroundThreadInfo * info, uint64_t interval)
{
    if constexpr (config::stats)
        ++info->tot_n_runs;
    info->npages_to_purge_new = 0;

    NsTime before_sleep;
    before_sleep.initUpdate();

    [[maybe_unused]] int ret;
    if (interval == BACKGROUND_THREAD_INDEFINITE_SLEEP)
    {
        info->wakeupTimeSet(tsdn, BACKGROUND_THREAD_INDEFINITE_SLEEP);
        ret = backgroundThreadCondWait(info, nullptr);
        JE_ASSERT(ret == 0);
    }
    else
    {
        JE_ASSERT(interval >= BACKGROUND_THREAD_MIN_INTERVAL_NS && interval <= BACKGROUND_THREAD_INDEFINITE_SLEEP);
        /// We need malloc clock (can be different from tv).
        NsTime next_wakeup;
        next_wakeup.initUpdate();
        next_wakeup.iadd(interval);
        JE_ASSERT(next_wakeup.ns() < BACKGROUND_THREAD_INDEFINITE_SLEEP);
        info->wakeupTimeSet(tsdn, next_wakeup.ns());

        /// The deadline is computed from the first clock read; the condition variable uses the same clock.
        NsTime ts_wakeup;
        ts_wakeup.copy(before_sleep);
        ts_wakeup.iadd(interval);
        struct timespec ts;
        ts.tv_sec = static_cast<time_t>(static_cast<size_t>(ts_wakeup.sec()));
        ts.tv_nsec = static_cast<long>(static_cast<size_t>(ts_wakeup.nsec()));

        JE_ASSERT(!info->indefiniteSleep());
        ret = backgroundThreadCondWait(info, &ts);
        JE_ASSERT(ret == ETIMEDOUT || ret == 0);
    }
    if constexpr (config::stats)
    {
        NsTime after_sleep;
        after_sleep.initUpdate();
        if (after_sleep.compare(before_sleep) > 0)
        {
            after_sleep.subtract(before_sleep);
            info->tot_sleep_time.add(after_sleep);
        }
    }
}

/// Returns true if the thread was paused (and has waited for the global lock).
/// jemalloc: background_thread_pause_check
bool backgroundThreadPauseCheck(ThreadState * tsdn, BackgroundThreadInfo * info)
{
    if (JE_UNLIKELY(info->state == BackgroundThreadState::Paused))
    {
        info->mtx.unlock(tsdn);
        /// Wait on global lock to update status.
        background_thread_lock.lock(tsdn);
        background_thread_lock.unlock(tsdn);
        info->mtx.lock(tsdn);
        return true;
    }

    return false;
}

/// The arena operations of a work pass of a background thread.
struct BackgroundWorkArenaOps
{
    ThreadState * tsdn;

    Arena * get(unsigned i) const { return arenaGet(tsdn, i, false); }
    void doWork(Arena * arena) const { arenaDoDeferredWork(tsdn, arena); }
    uint64_t timeUntilDeferredWork(Arena * arena) const { return arena->pa_shard.timeUntilDeferredWork(tsdn); }
};

/// jemalloc: background_work_sleep_once
void backgroundWorkSleepOnce(ThreadState * tsdn, BackgroundThreadInfo * info, unsigned ind)
{
    unsigned narenas = narenasTotalGet();
    bool slept_indefinitely = info->indefiniteSleep();

    uint64_t sleep_ns = backgroundWorkPass(ind, narenas, max_background_threads, slept_indefinitely, BackgroundWorkArenaOps{tsdn});

    backgroundThreadSleep(tsdn, info, sleep_ns);
}

/// Returns true if joining the thread failed.
/// jemalloc: background_threads_disable_single
bool backgroundThreadsDisableSingle(ThreadState & tsd, BackgroundThreadInfo * info)
{
    if (info == &background_thread_info[0])
        background_thread_lock.assertOwner(&tsd);
    else
        background_thread_lock.assertNotOwner(&tsd);

    preReentrancy(tsd, nullptr);
    info->mtx.lock(&tsd);
    bool has_thread;
    JE_ASSERT(info->state != BackgroundThreadState::Paused);
    if (info->state == BackgroundThreadState::Started)
    {
        has_thread = true;
        info->state = BackgroundThreadState::Stopped;
        pthread_cond_signal(&info->cond);
    }
    else
    {
        has_thread = false;
    }
    info->mtx.unlock(&tsd);

    if (!has_thread)
    {
        postReentrancy(tsd);
        return false;
    }
    void * ret;
    if (pthread_join(info->thread, &ret))
    {
        postReentrancy(tsd);
        return true;
    }
    JE_ASSERT(ret == nullptr);
    --n_background_threads;
    postReentrancy(tsd);

    return false;
}

void * backgroundThreadEntry(void * ind_arg);

/// Mask signals during thread creation so that the thread inherits an empty signal set.
/// jemalloc: background_thread_create_signals_masked
int backgroundThreadCreateSignalsMasked(
    pthread_t * thread, const pthread_attr_t * attr, void * (*start_routine)(void *), void * arg)
{
    sigset_t set;
    sigfillset(&set);
    sigset_t oldset;
    int mask_err = pthread_sigmask(SIG_SETMASK, &set, &oldset);
    if (mask_err != 0)
        return mask_err;
    int create_err = pthreadCreateWrapper(thread, attr, start_routine, arg);
    /// Restore the signal mask. Failure to restore the signal mask here changes program behavior.
    int restore_err = pthread_sigmask(SIG_SETMASK, &oldset, nullptr);
    if (restore_err != 0)
    {
        printMessage(
            "<jemalloc>: background thread creation failed (%d), and signal mask restoration failed (%d)\n",
            create_err,
            restore_err);
        if (opt.abort)
            abort();
    }
    return create_err;
}

/// Run by thread 0 holding `background_thread_info[0].mtx`: creates (at most) one of the started but not yet
/// created threads. Returns true if it unlocked the mutex (the caller restarts its loop).
/// jemalloc: check_background_thread_creation
bool checkBackgroundThreadCreation(
    ThreadState & tsd, const size_t const_max_background_threads, unsigned * n_created, bool * created_threads)
{
    bool ret = false;
    if (JE_LIKELY(*n_created == n_background_threads))
        return ret;

    ThreadState * tsdn = &tsd;
    background_thread_info[0].mtx.unlock(tsdn);
    for (unsigned i = 1; i < const_max_background_threads; ++i)
    {
        if (created_threads[i])
            continue;
        BackgroundThreadInfo * info = &background_thread_info[i];
        info->mtx.lock(tsdn);
        /// In case of the background_thread_paused state because of arena reset, delay the creation.
        bool create = (info->state == BackgroundThreadState::Started);
        info->mtx.unlock(tsdn);
        if (!create)
            continue;

        preReentrancy(tsd, nullptr);
        int err = backgroundThreadCreateSignalsMasked(
            &info->thread, nullptr, backgroundThreadEntry, reinterpret_cast<void *>(static_cast<uintptr_t>(i)));
        postReentrancy(tsd);

        if (err == 0)
        {
            ++(*n_created);
            created_threads[i] = true;
        }
        else
        {
            printMessage("<jemalloc>: background thread creation failed (%d)\n", err);
            if (opt.abort)
                abort();
        }
        /// Return to restart the loop since we unlocked.
        ret = true;
        break;
    }
    background_thread_info[0].mtx.lock(tsdn);

    return ret;
}

/// Thread 0 is also responsible for launching / terminating threads.
/// jemalloc: background_thread0_work
void backgroundThread0Work(ThreadState & tsd)
{
    /// `max_background_threads` does not change underneath us.
    const size_t const_max_background_threads = max_background_threads;
    JE_ASSERT(const_max_background_threads > 0);
    /// jemalloc uses a variable-length array of `max_background_threads` (at most `MAX_BACKGROUND_THREAD_LIMIT`).
    std::array<bool, MAX_BACKGROUND_THREAD_LIMIT> created_threads;
    unsigned i;
    for (i = 1; i < const_max_background_threads; ++i)
        created_threads[i] = false;
    /// Start working, and create more threads when asked.
    unsigned n_created = 1;
    while (background_thread_info[0].state != BackgroundThreadState::Stopped)
    {
        if (backgroundThreadPauseCheck(&tsd, &background_thread_info[0]))
            continue;
        if (checkBackgroundThreadCreation(tsd, const_max_background_threads, &n_created, created_threads.data()))
            continue;
        backgroundWorkSleepOnce(&tsd, &background_thread_info[0], 0);
    }

    /// Shut down other threads at exit. Note that the ctl thread is holding the global background_thread mutex (and is
    /// waiting) for us.
    JE_ASSERT(!backgroundThreadEnabled());
    for (i = 1; i < const_max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        JE_ASSERT(info->state != BackgroundThreadState::Paused);
        if (created_threads[i])
        {
            backgroundThreadsDisableSingle(tsd, info);
        }
        else
        {
            info->mtx.lock(&tsd);
            if (info->state != BackgroundThreadState::Stopped)
            {
                /// The thread was not created.
                JE_ASSERT(info->state == BackgroundThreadState::Started);
                --n_background_threads;
                info->state = BackgroundThreadState::Stopped;
            }
            info->mtx.unlock(&tsd);
        }
    }
    background_thread_info[0].state = BackgroundThreadState::Stopped;
    JE_ASSERT(n_background_threads == 1);
}

/// jemalloc: background_work
void backgroundWork(ThreadState & tsd, unsigned ind)
{
    BackgroundThreadInfo * info = &background_thread_info[ind];

    info->mtx.lock(&tsd);
    /// The first pass is treated as a wakeup from an indefinite sleep: it does no work, only scheduling.
    info->wakeupTimeSet(&tsd, BACKGROUND_THREAD_INDEFINITE_SLEEP);
    if (ind == 0)
    {
        backgroundThread0Work(tsd);
    }
    else
    {
        while (info->state != BackgroundThreadState::Stopped)
        {
            if (backgroundThreadPauseCheck(&tsd, info))
                continue;
            backgroundWorkSleepOnce(&tsd, info, ind);
        }
    }
    JE_ASSERT(info->state == BackgroundThreadState::Stopped);
    info->wakeupTimeSet(&tsd, 0);
    info->mtx.unlock(&tsd);
}

/// jemalloc: background_thread_entry
void * backgroundThreadEntry(void * ind_arg)
{
    unsigned thread_ind = static_cast<unsigned>(reinterpret_cast<uintptr_t>(ind_arg));
    JE_ASSERT(thread_ind < max_background_threads);
    setCurrentThreadName();
    if (opt.percpu_arena != PercpuArenaMode::Disabled)
        setCurrentThreadAffinity(static_cast<int>(thread_ind));
    /// Start periodic background work. We use internal tsd which avoids side effects, for example triggering new
    /// arena creation (which in turn triggers another background thread creation).
    backgroundWork(ThreadState::internalFetch(), thread_ind);
    JE_ASSERT(pthread_equal(pthread_self(), background_thread_info[thread_ind].thread));

    return nullptr;
}

/// Requires `background_thread_lock` and `info->mtx`.
/// jemalloc: background_thread_init
void backgroundThreadInit(ThreadState & tsd, BackgroundThreadInfo * info)
{
    background_thread_lock.assertOwner(&tsd);
    info->state = BackgroundThreadState::Started;
    backgroundThreadInfoInit(&tsd, info);
    ++n_background_threads;
}

/// jemalloc: background_thread_create_locked
bool backgroundThreadCreateLocked(ThreadState & tsd, unsigned arena_ind)
{
    background_thread_lock.assertOwner(&tsd);

    /// We create at most NCPUs threads.
    size_t thread_ind = arena_ind % max_background_threads;
    BackgroundThreadInfo * info = &background_thread_info[thread_ind];

    bool need_new_thread;
    info->mtx.lock(&tsd);
    /// The last check is there to leave Thread 0 creation entirely to the initializing thread (arena 0).
    need_new_thread = backgroundThreadEnabled() && (info->state == BackgroundThreadState::Stopped)
        && (thread_ind != 0 || arena_ind == 0);
    if (need_new_thread)
        backgroundThreadInit(tsd, info);
    info->mtx.unlock(&tsd);
    if (!need_new_thread)
        return false;
    if (arena_ind != 0)
    {
        /// Threads are created asynchronously by Thread 0.
        BackgroundThreadInfo * t0 = &background_thread_info[0];
        t0->mtx.lock(&tsd);
        pthread_cond_signal(&t0->cond);
        t0->mtx.unlock(&tsd);

        return false;
    }

    preReentrancy(tsd, nullptr);
    /// To avoid complications (besides reentrancy), create internal background threads with the underlying
    /// pthread_create.
    int err = backgroundThreadCreateSignalsMasked(&info->thread, nullptr, backgroundThreadEntry, reinterpret_cast<void *>(thread_ind));
    postReentrancy(tsd);

    if (err != 0)
    {
        /// ClickHouse filters this exact message (`programs/main.cpp`).
        printMessage("<jemalloc>: arena 0 background thread creation failed (%d)\n", err);
        info->mtx.lock(&tsd);
        info->state = BackgroundThreadState::Stopped;
        --n_background_threads;
        info->mtx.unlock(&tsd);

        return true;
    }

    return false;
}

}

/// --- Public functions ----------------------------------------------------------------------------------------------

/// jemalloc: background_thread_create
bool backgroundThreadCreate(ThreadState & tsd, unsigned arena_ind)
{
    static_assert(config::background_thread);

    background_thread_lock.lock(&tsd);
    bool ret = backgroundThreadCreateLocked(tsd, arena_ind);
    background_thread_lock.unlock(&tsd);

    return ret;
}

/// jemalloc: background_threads_enable
bool backgroundThreadsEnable(ThreadState & tsd)
{
    JE_ASSERT(n_background_threads == 0);
    JE_ASSERT(backgroundThreadEnabled());
    background_thread_lock.assertOwner(&tsd);

    /// jemalloc uses a variable-length array of `max_background_threads` (at most `MAX_BACKGROUND_THREAD_LIMIT`).
    std::array<bool, MAX_BACKGROUND_THREAD_LIMIT> marked;
    unsigned nmarked;
    for (size_t i = 0; i < max_background_threads; ++i)
        marked[i] = false;
    nmarked = 0;
    /// Thread 0 is required and created at the end.
    marked[0] = true;
    /// Mark the threads we need to create for thread 0.
    unsigned narenas = narenasTotalGet();
    for (unsigned i = 1; i < narenas; ++i)
    {
        if (marked[i % max_background_threads] || arenaGet(&tsd, i, false) == nullptr)
            continue;
        BackgroundThreadInfo * info = &background_thread_info[i % max_background_threads];
        info->mtx.lock(&tsd);
        JE_ASSERT(info->state == BackgroundThreadState::Stopped);
        backgroundThreadInit(tsd, info);
        info->mtx.unlock(&tsd);
        marked[i % max_background_threads] = true;
        if (++nmarked == max_background_threads)
            break;
    }

    bool err = backgroundThreadCreateLocked(tsd, 0);
    if (err)
        return true;
    for (unsigned i = 0; i < narenas; ++i)
    {
        Arena * arena = arenaGet(&tsd, i, false);
        if (arena != nullptr)
            arena->pa_shard.setDeferralAllowed(&tsd, true);
    }
    return false;
}

/// jemalloc: background_threads_disable
bool backgroundThreadsDisable(ThreadState & tsd)
{
    JE_ASSERT(!backgroundThreadEnabled());
    background_thread_lock.assertOwner(&tsd);

    /// Thread 0 will be responsible for terminating other threads.
    if (backgroundThreadsDisableSingle(tsd, &background_thread_info[0]))
        return true;
    JE_ASSERT(n_background_threads == 0);
    unsigned narenas = narenasTotalGet();
    for (unsigned i = 0; i < narenas; ++i)
    {
        Arena * arena = arenaGet(&tsd, i, false);
        if (arena != nullptr)
            arena->pa_shard.setDeferralAllowed(&tsd, false);
    }

    return false;
}

/// jemalloc: background_thread_is_started
bool backgroundThreadIsStarted(BackgroundThreadInfo * info)
{
    return info->state == BackgroundThreadState::Started;
}

/// jemalloc: background_thread_wakeup_early
void backgroundThreadWakeupEarly(BackgroundThreadInfo * info, NsTime * remaining_sleep)
{
    /// This is an optimization to increase batching. At this point we know that background thread wakes up soon, so
    /// the time to cache the just freed memory is bounded and low.
    if (remaining_sleep != nullptr && remaining_sleep->ns() < BACKGROUND_THREAD_MIN_INTERVAL_NS)
        return;
    pthread_cond_signal(&info->cond);
}

/// jemalloc: background_thread_prefork0
void backgroundThreadPrefork0(ThreadState * tsdn)
{
    background_thread_lock.prefork(tsdn);
    background_thread_enabled_at_fork = backgroundThreadEnabled();
}

/// jemalloc: background_thread_prefork1
void backgroundThreadPrefork1(ThreadState * tsdn)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mtx.prefork(tsdn);
}

/// jemalloc: background_thread_postfork_parent
void backgroundThreadPostforkParent(ThreadState * tsdn)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mtx.postforkParent(tsdn);
    background_thread_lock.postforkParent(tsdn);
}

/// jemalloc: background_thread_postfork_child
void backgroundThreadPostforkChild(ThreadState * tsdn)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mtx.postforkChild(tsdn);
    background_thread_lock.postforkChild(tsdn);
    if (!background_thread_enabled_at_fork)
        return;

    /// Clear background_thread state (reset to disabled for child).
    background_thread_lock.lock(tsdn);
    n_background_threads = 0;
    backgroundThreadEnabledSet(tsdn, false);
    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        info->mtx.lock(tsdn);
        info->state = BackgroundThreadState::Stopped;
        [[maybe_unused]] int ret = condInit(&info->cond, /* fallback_to_default */ true);
        JE_ASSERT(ret == 0);
        backgroundThreadInfoInit(tsdn, info);
        info->mtx.unlock(tsdn);
    }
    background_thread_lock.unlock(tsdn);
}

/// jemalloc: background_thread_stats_read
bool backgroundThreadStatsRead(ThreadState * tsdn, BackgroundThreadStats * stats)
{
    static_assert(config::stats);
    background_thread_lock.lock(tsdn);
    if (!backgroundThreadEnabled())
    {
        background_thread_lock.unlock(tsdn);
        return true;
    }

    stats->run_interval.initZero();
    stats->max_counter_per_bg_thd.reset();

    uint64_t num_runs = 0;
    stats->num_threads = n_background_threads;
    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        if (!info->mtx.tryLock(tsdn))
        {
            /// Each background thread run may take a long time; avoid waiting on the stats if the thread is active.
            continue;
        }
        if (info->state != BackgroundThreadState::Stopped)
        {
            num_runs += info->tot_n_runs;
            stats->run_interval.add(info->tot_sleep_time);
            info->mtx.profMaxUpdate(tsdn, stats->max_counter_per_bg_thd);
        }
        info->mtx.unlock(tsdn);
    }
    stats->num_runs = num_runs;
    if (num_runs > 0)
        stats->run_interval.idivide(num_runs);
    background_thread_lock.unlock(tsdn);

    return false;
}

/// When lazy lock is enabled, we need to make sure setting isthreaded before taking any background_thread locks. This
/// is called early in ctl (instead of wait for the pthread_create calls to trigger) because the mutex is required
/// before creating background threads.
/// jemalloc: background_thread_ctl_init
void backgroundThreadCtlInit(ThreadState * tsdn)
{
    background_thread_lock.assertNotOwner(tsdn);
    pthreadCreateFptrInit();
    pthreadCreateWrapperInit();
}

/// jemalloc: pthread_create_wrapper
int pthreadCreateWrapper(pthread_t * thread, const pthread_attr_t * attr, void * (*start_routine)(void *), void * arg)
{
    pthreadCreateWrapperInit();

    return pthread_create_fptr(thread, attr, start_routine, arg);
}

/// jemalloc: background_thread_boot0
bool backgroundThreadBoot0()
{
    /// `!have_background_thread && opt_background_thread` ("option background_thread currently supports pthread
    /// only") cannot happen: background threads are supported on all platforms.
    static_assert(config::background_thread);
    /// `JEMALLOC_PTHREAD_CREATE_WRAPPER` is defined everywhere (`JEMALLOC_BACKGROUND_THREAD`).
    if ((config::lazy_lock || opt.background_thread) && pthreadCreateFptrInit())
        return true;
    return false;
}

/// jemalloc: background_thread_boot1
bool backgroundThreadBoot1(ThreadState * tsdn, Base * base)
{
    JE_ASSERT(narenasTotalGet() > 0);

    if (opt.max_background_threads > MAX_BACKGROUND_THREAD_LIMIT)
        opt.max_background_threads = DEFAULT_NUM_BACKGROUND_THREAD;
    max_background_threads = opt.max_background_threads;

    if (background_thread_lock.init("background_thread_global", MutexRank::BACKGROUND_THREAD_GLOBAL, MutexLockOrder::RankExclusive))
        return true;

    background_thread_info = static_cast<BackgroundThreadInfo *>(
        base->alloc(tsdn, opt.max_background_threads * sizeof(BackgroundThreadInfo), CACHELINE));
    if (background_thread_info == nullptr)
        return true;

    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = new (&background_thread_info[i]) BackgroundThreadInfo;
        /// Thread mutex is rank_inclusive because of thread0.
        if (info->mtx.init("background_thread", MutexRank::BACKGROUND_THREAD, MutexLockOrder::AddressOrdered))
            return true;
        if (condInit(&info->cond, /* fallback_to_default */ false))
            return true;
        info->mtx.lock(tsdn);
        info->state = BackgroundThreadState::Stopped;
        backgroundThreadInfoInit(tsdn, info);
        info->mtx.unlock(tsdn);
    }
    /// Using `Impl` to bypass the locking check during init.
    backgroundThreadEnabledSetImpl(opt.background_thread);
    return false;
}

/// --- Hooks of the arena module (declared in Arena.h) ---------------------------------------------------------------

/// jemalloc: arena_background_thread_info_get
BackgroundThreadInfo * arenaBackgroundThreadInfoGet(Arena * arena)
{
    unsigned arena_ind = arenaIndGet(arena);
    return &background_thread_info[arena_ind % max_background_threads];
}

/// `&info->mtx`
Mutex & backgroundThreadInfoMutex(BackgroundThreadInfo * info)
{
    return info->mtx;
}

/// jemalloc: background_thread_indefinite_sleep
bool backgroundThreadIndefiniteSleep(BackgroundThreadInfo * info)
{
    return info->indefiniteSleep();
}

/// jemalloc: background_thread_wakeup_time_get
uint64_t backgroundThreadWakeupTimeGet(BackgroundThreadInfo * info)
{
    return info->wakeupTimeGet();
}

/// `info->npages_to_purge_new`
size_t & backgroundThreadNpagesToPurgeNew(BackgroundThreadInfo * info)
{
    return info->npages_to_purge_new;
}

}

#if defined(__FreeBSD__)
static_assert(jemalloc::config::lazy_lock);
/// We intercept `pthread_create` calls in order to toggle `isthreaded` if the process goes multi-threaded
/// (`JEMALLOC_LAZY_LOCK`). jemalloc: pthread_create (`src/mutex.c`)
extern "C" __attribute__((visibility("default"))) int
pthread_create(pthread_t * __restrict thread, const pthread_attr_t * __restrict attr, void * (*start_routine)(void *), void * __restrict arg)
{
    return jemalloc::pthreadCreateWrapper(thread, attr, start_routine, arg);
}
#else
static_assert(!jemalloc::config::lazy_lock);
#endif
