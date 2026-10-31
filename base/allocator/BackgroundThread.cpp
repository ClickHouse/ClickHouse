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
#include <new>
#include <dlfcn.h>
#include <pthread.h>
#include <sched.h>

#if defined(__FreeBSD__)
#include <pthread_np.h>
#endif

namespace jemalloc
{

/// --- Data ----------------------------------------------------------------------------------------------------------

constinit Mutex background_thread_lock;
constinit std::atomic<bool> background_thread_enabled_state{false};
constinit size_t num_background_threads = 0;
constinit size_t max_background_threads = 0;
constinit BackgroundThreadInfo * background_thread_info = nullptr;

namespace
{

/// jemalloc: background_thread_enabled_at_fork
constinit bool background_thread_enabled_at_fork = false;

using PthreadCreateFunction = int (*)(pthread_t *, const pthread_attr_t *, void * (*)(void *), void *);

/// jemalloc: pthread_create_fptr
constinit PthreadCreateFunction pthread_create_function_ptr = nullptr;

/// jemalloc: pthread_create_wrapper_init
void pthreadCreateWrapperInit()
{
    if constexpr (config::lazy_lock)
    {
        if (!is_threaded)
            is_threaded = true;
    }
}

/// Returns true on error (never: a failed lookup aborts or falls back).
/// jemalloc: pthread_create_fptr_init
bool pthreadCreateFunctionPtrInit()
{
    if (pthread_create_function_ptr != nullptr)
        return false;
    /// Try the next symbol first, because 1) when use lazy_lock we have a wrapper for pthread_create; and 2)
    /// application may define its own wrapper as well (and can call malloc within the wrapper).
    pthread_create_function_ptr = reinterpret_cast<PthreadCreateFunction>(dlsym(RTLD_NEXT, "pthread_create"));
    if (pthread_create_function_ptr == nullptr)
        pthread_create_function_ptr = reinterpret_cast<PthreadCreateFunction>(dlsym(RTLD_DEFAULT, "pthread_create"));
    if (pthread_create_function_ptr == nullptr)
    {
        if constexpr (config::lazy_lock)
        {
            writeMessage("<jemalloc>: Error in dlsym(RTLD_NEXT, \"pthread_create\")\n");
            abort();
        }
        else
        {
            /// Fall back to the default symbol.
            pthread_create_function_ptr = pthread_create;
        }
    }

    return false;
}

/// Initializes the condition variable of a thread info with the clock of `Nanoseconds` (`CLOCK_MONOTONIC` where
/// available). At boot any failure is an error; after fork (`fallback_to_default`) the default attributes
/// (`CLOCK_REALTIME`) are used if the clock cannot be set (fork patch 861db0b4). Returns the error of
/// `pthread_cond_init` (non-zero also for an attribute failure at boot).
/// jemalloc: the condition variable initialization in background_thread_boot1 and background_thread_postfork_child
int conditionInit(pthread_cond_t * condition, bool fallback_to_default)
{
#if !defined(__APPLE__)
    static_assert(config::have_clock_monotonic);
    pthread_condattr_t condition_attributes;
    if (pthread_condattr_init(&condition_attributes))
        return fallback_to_default ? pthread_cond_init(condition, nullptr) : 1;
    if (pthread_condattr_setclock(&condition_attributes, CLOCK_MONOTONIC))
    {
        /// Fall back to default (CLOCK_REALTIME) attributes if setclock fails.
        pthread_condattr_destroy(&condition_attributes);
        return fallback_to_default ? pthread_cond_init(condition, nullptr) : 1;
    }
    int result = pthread_cond_init(condition, &condition_attributes);
    pthread_condattr_destroy(&condition_attributes);
    return result;
#else
    static_assert(!config::have_clock_monotonic);
    (void)fallback_to_default;
    return pthread_cond_init(condition, nullptr);
#endif
}

/// jemalloc: background_thread_info_init
void backgroundThreadInfoInit(ThreadState * thread_state, BackgroundThreadInfo * info)
{
    info->wakeupTimeSet(thread_state, 0);
    info->num_pages_to_purge_new = 0;
    if constexpr (config::stats)
    {
        info->total_num_runs = 0;
        info->total_sleep_time.initZero();
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
int backgroundThreadConditionWait(BackgroundThreadInfo * info, const struct timespec * time_spec)
{
    int result;

    info->mutex.setLockedFlag(false);
    if (time_spec == nullptr)
        result = pthread_cond_wait(&info->condition, info->mutex.nativeHandle());
    else
        result = pthread_cond_timedwait(&info->condition, info->mutex.nativeHandle(), time_spec);
    info->mutex.setLockedFlag(true);

    return result;
}

/// jemalloc: background_thread_sleep
void backgroundThreadSleep(ThreadState * thread_state, BackgroundThreadInfo * info, uint64_t interval)
{
    if constexpr (config::stats)
        ++info->total_num_runs;
    info->num_pages_to_purge_new = 0;

    Nanoseconds before_sleep;
    before_sleep.initUpdate();

    [[maybe_unused]] int result;
    if (interval == BACKGROUND_THREAD_INDEFINITE_SLEEP)
    {
        info->wakeupTimeSet(thread_state, BACKGROUND_THREAD_INDEFINITE_SLEEP);
        result = backgroundThreadConditionWait(info, nullptr);
        ALLOCATOR_ASSERT(result == 0);
    }
    else
    {
        ALLOCATOR_ASSERT(interval >= BACKGROUND_THREAD_MIN_INTERVAL_NS && interval <= BACKGROUND_THREAD_INDEFINITE_SLEEP);
        /// We need malloc clock (can be different from tv).
        Nanoseconds next_wakeup;
        next_wakeup.initUpdate();
        next_wakeup.addNanoseconds(interval);
        ALLOCATOR_ASSERT(next_wakeup.ns() < BACKGROUND_THREAD_INDEFINITE_SLEEP);
        info->wakeupTimeSet(thread_state, next_wakeup.ns());

        /// The deadline is computed from the first clock read; the condition variable uses the same clock.
        Nanoseconds wakeup_time;
        wakeup_time.copy(before_sleep);
        wakeup_time.addNanoseconds(interval);
        struct timespec time_spec;
        time_spec.tv_sec = static_cast<time_t>(static_cast<size_t>(wakeup_time.seconds()));
        time_spec.tv_nsec = static_cast<long>(static_cast<size_t>(wakeup_time.nsec()));

        ALLOCATOR_ASSERT(!info->indefiniteSleep());
        result = backgroundThreadConditionWait(info, &time_spec);
        ALLOCATOR_ASSERT(result == ETIMEDOUT || result == 0);
    }
    if constexpr (config::stats)
    {
        Nanoseconds after_sleep;
        after_sleep.initUpdate();
        if (after_sleep.compare(before_sleep) > 0)
        {
            after_sleep.subtract(before_sleep);
            info->total_sleep_time.add(after_sleep);
        }
    }
}

/// Returns true if the thread was paused (and has waited for the global lock).
/// jemalloc: background_thread_pause_check
bool backgroundThreadPauseCheck(ThreadState * thread_state, BackgroundThreadInfo * info)
{
    if (ALLOCATOR_UNLIKELY(info->state == BackgroundThreadState::Paused))
    {
        info->mutex.unlock(thread_state);
        /// Wait on global lock to update status.
        background_thread_lock.lock(thread_state);
        background_thread_lock.unlock(thread_state);
        info->mutex.lock(thread_state);
        return true;
    }

    return false;
}

/// The arena operations of a work pass of a background thread.
struct BackgroundWorkArenaOps
{
    ThreadState * thread_state;

    Arena * get(unsigned i) const { return arenaGet(thread_state, i, false); }
    void doWork(Arena * arena) const { arenaDoDeferredWork(thread_state, arena); }
    uint64_t timeUntilDeferredWork(Arena * arena) const { return arena->page_allocator_shard.timeUntilDeferredWork(thread_state); }
};

/// jemalloc: background_work_sleep_once
void backgroundWorkSleepOnce(ThreadState * thread_state, BackgroundThreadInfo * info, unsigned idx)
{
    unsigned num_arenas = numArenasTotalGet();
    bool slept_indefinitely = info->indefiniteSleep();

    uint64_t sleep_ns
        = backgroundWorkPass(idx, num_arenas, max_background_threads, slept_indefinitely, BackgroundWorkArenaOps{thread_state});

    backgroundThreadSleep(thread_state, info, sleep_ns);
}

/// Returns true if joining the thread failed.
/// jemalloc: background_threads_disable_single
bool backgroundThreadsDisableSingle(ThreadState & thread_state, BackgroundThreadInfo * info)
{
    if (info == &background_thread_info[0])
        background_thread_lock.assertOwner(&thread_state);
    else
        background_thread_lock.assertNotOwner(&thread_state);

    preReentrancy(thread_state, nullptr);
    info->mutex.lock(&thread_state);
    bool has_thread;
    ALLOCATOR_ASSERT(info->state != BackgroundThreadState::Paused);
    if (info->state == BackgroundThreadState::Started)
    {
        has_thread = true;
        info->state = BackgroundThreadState::Stopped;
        pthread_cond_signal(&info->condition);
    }
    else
    {
        has_thread = false;
    }
    info->mutex.unlock(&thread_state);

    if (!has_thread)
    {
        postReentrancy(thread_state);
        return false;
    }
    void * result;
    if (pthread_join(info->thread, &result))
    {
        postReentrancy(thread_state);
        return true;
    }
    ALLOCATOR_ASSERT(result == nullptr);
    --num_background_threads;
    postReentrancy(thread_state);

    return false;
}

void * backgroundThreadEntry(void * idx_arg);

/// Mask signals during thread creation so that the thread inherits an empty signal set.
/// jemalloc: background_thread_create_signals_masked
int backgroundThreadCreateSignalsMasked(pthread_t * thread, const pthread_attr_t * attributes, void * (*start_routine)(void *), void * arg)
{
    sigset_t set;
    sigfillset(&set);
    sigset_t old_set;
    int mask_error = pthread_sigmask(SIG_SETMASK, &set, &old_set);
    if (mask_error != 0)
        return mask_error;
    int create_error = pthreadCreateWrapper(thread, attributes, start_routine, arg);
    /// Restore the signal mask. Failure to restore the signal mask here changes program behavior.
    int restore_error = pthread_sigmask(SIG_SETMASK, &old_set, nullptr);
    if (restore_error != 0)
    {
        printMessage(
            "<jemalloc>: background thread creation failed (%d), and signal mask restoration failed (%d)\n", create_error, restore_error);
        if (options.abort)
            abort();
    }
    return create_error;
}

/// Run by thread 0 holding `background_thread_info[0].mtx`: creates (at most) one of the started but not yet
/// created threads. Returns true if it unlocked the mutex (the caller restarts its loop).
/// jemalloc: check_background_thread_creation
bool checkBackgroundThreadCreation(
    ThreadState & thread_state, const size_t const_max_background_threads, unsigned * num_created, bool * created_threads)
{
    bool result = false;
    if (ALLOCATOR_LIKELY(*num_created == num_background_threads))
        return result;

    ThreadState * thread_state_ptr = &thread_state;
    background_thread_info[0].mutex.unlock(thread_state_ptr);
    for (unsigned i = 1; i < const_max_background_threads; ++i)
    {
        if (created_threads[i])
            continue;
        BackgroundThreadInfo * info = &background_thread_info[i];
        info->mutex.lock(thread_state_ptr);
        /// In case of the background_thread_paused state because of arena reset, delay the creation.
        bool create = (info->state == BackgroundThreadState::Started);
        info->mutex.unlock(thread_state_ptr);
        if (!create)
            continue;

        preReentrancy(thread_state, nullptr);
        int error = backgroundThreadCreateSignalsMasked(
            &info->thread, nullptr, backgroundThreadEntry, reinterpret_cast<void *>(static_cast<uintptr_t>(i)));
        postReentrancy(thread_state);

        if (error == 0)
        {
            ++(*num_created);
            created_threads[i] = true;
        }
        else
        {
            printMessage("<jemalloc>: background thread creation failed (%d)\n", error);
            if (options.abort)
                abort();
        }
        /// Return to restart the loop since we unlocked.
        result = true;
        break;
    }
    background_thread_info[0].mutex.lock(thread_state_ptr);

    return result;
}

/// Thread 0 is also responsible for launching / terminating threads.
/// jemalloc: background_thread0_work
void backgroundThread0Work(ThreadState & thread_state)
{
    /// `max_background_threads` does not change underneath us.
    const size_t const_max_background_threads = max_background_threads;
    ALLOCATOR_ASSERT(const_max_background_threads > 0);
    /// jemalloc uses a variable-length array of `max_background_threads` (at most `MAX_BACKGROUND_THREAD_LIMIT`).
    std::array<bool, MAX_BACKGROUND_THREAD_LIMIT> created_threads;
    unsigned i;
    for (i = 1; i < const_max_background_threads; ++i)
        created_threads[i] = false;
    /// Start working, and create more threads when asked.
    unsigned num_created = 1;
    while (background_thread_info[0].state != BackgroundThreadState::Stopped)
    {
        if (backgroundThreadPauseCheck(&thread_state, &background_thread_info[0]))
            continue;
        if (checkBackgroundThreadCreation(thread_state, const_max_background_threads, &num_created, created_threads.data()))
            continue;
        backgroundWorkSleepOnce(&thread_state, &background_thread_info[0], 0);
    }

    /// Shut down other threads at exit. Note that the ctl thread is holding the global background_thread mutex (and is
    /// waiting) for us.
    ALLOCATOR_ASSERT(!backgroundThreadEnabled());
    for (i = 1; i < const_max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        ALLOCATOR_ASSERT(info->state != BackgroundThreadState::Paused);
        if (created_threads[i])
        {
            backgroundThreadsDisableSingle(thread_state, info);
        }
        else
        {
            info->mutex.lock(&thread_state);
            if (info->state != BackgroundThreadState::Stopped)
            {
                /// The thread was not created.
                ALLOCATOR_ASSERT(info->state == BackgroundThreadState::Started);
                --num_background_threads;
                info->state = BackgroundThreadState::Stopped;
            }
            info->mutex.unlock(&thread_state);
        }
    }
    background_thread_info[0].state = BackgroundThreadState::Stopped;
    ALLOCATOR_ASSERT(num_background_threads == 1);
}

/// jemalloc: background_work
void backgroundWork(ThreadState & thread_state, unsigned idx)
{
    BackgroundThreadInfo * info = &background_thread_info[idx];

    info->mutex.lock(&thread_state);
    /// The first pass is treated as a wakeup from an indefinite sleep: it does no work, only scheduling.
    info->wakeupTimeSet(&thread_state, BACKGROUND_THREAD_INDEFINITE_SLEEP);
    if (idx == 0)
    {
        backgroundThread0Work(thread_state);
    }
    else
    {
        while (info->state != BackgroundThreadState::Stopped)
        {
            if (backgroundThreadPauseCheck(&thread_state, info))
                continue;
            backgroundWorkSleepOnce(&thread_state, info, idx);
        }
    }
    ALLOCATOR_ASSERT(info->state == BackgroundThreadState::Stopped);
    info->wakeupTimeSet(&thread_state, 0);
    info->mutex.unlock(&thread_state);
}

/// jemalloc: background_thread_entry
void * backgroundThreadEntry(void * idx_arg)
{
    unsigned thread_idx = static_cast<unsigned>(reinterpret_cast<uintptr_t>(idx_arg));
    ALLOCATOR_ASSERT(thread_idx < max_background_threads);
    setCurrentThreadName();
    if (options.per_cpu_arena != PerCPUArenaMode::Disabled)
        setCurrentThreadAffinity(static_cast<int>(thread_idx));
    /// Start periodic background work. We use internal tsd which avoids side effects, for example triggering new
    /// arena creation (which in turn triggers another background thread creation).
    backgroundWork(ThreadState::internalFetch(), thread_idx);
    ALLOCATOR_ASSERT(pthread_equal(pthread_self(), background_thread_info[thread_idx].thread));

    return nullptr;
}

/// Requires `background_thread_lock` and `info->mutex`.
/// jemalloc: background_thread_init
void backgroundThreadInit(ThreadState & thread_state, BackgroundThreadInfo * info)
{
    background_thread_lock.assertOwner(&thread_state);
    info->state = BackgroundThreadState::Started;
    backgroundThreadInfoInit(&thread_state, info);
    ++num_background_threads;
}

/// jemalloc: background_thread_create_locked
bool backgroundThreadCreateLocked(ThreadState & thread_state, unsigned arena_idx)
{
    background_thread_lock.assertOwner(&thread_state);

    /// We create at most NCPUs threads.
    size_t thread_idx = arena_idx % max_background_threads;
    BackgroundThreadInfo * info = &background_thread_info[thread_idx];

    bool need_new_thread;
    info->mutex.lock(&thread_state);
    /// The last check is there to leave Thread 0 creation entirely to the initializing thread (arena 0).
    need_new_thread = backgroundThreadEnabled() && (info->state == BackgroundThreadState::Stopped) && (thread_idx != 0 || arena_idx == 0);
    if (need_new_thread)
        backgroundThreadInit(thread_state, info);
    info->mutex.unlock(&thread_state);
    if (!need_new_thread)
        return false;
    if (arena_idx != 0)
    {
        /// Threads are created asynchronously by Thread 0.
        BackgroundThreadInfo * t0 = &background_thread_info[0];
        t0->mutex.lock(&thread_state);
        pthread_cond_signal(&t0->condition);
        t0->mutex.unlock(&thread_state);

        return false;
    }

    preReentrancy(thread_state, nullptr);
    /// To avoid complications (besides reentrancy), create internal background threads with the underlying
    /// pthread_create.
    int error = backgroundThreadCreateSignalsMasked(&info->thread, nullptr, backgroundThreadEntry, reinterpret_cast<void *>(thread_idx));
    postReentrancy(thread_state);

    if (error != 0)
    {
        /// ClickHouse filters this exact message (`programs/main.cpp`).
        printMessage("<jemalloc>: arena 0 background thread creation failed (%d)\n", error);
        info->mutex.lock(&thread_state);
        info->state = BackgroundThreadState::Stopped;
        --num_background_threads;
        info->mutex.unlock(&thread_state);

        return true;
    }

    return false;
}

}

/// --- Public functions ----------------------------------------------------------------------------------------------

/// jemalloc: background_thread_create
bool backgroundThreadCreate(ThreadState & thread_state, unsigned arena_idx)
{
    static_assert(config::background_thread);

    background_thread_lock.lock(&thread_state);
    bool result = backgroundThreadCreateLocked(thread_state, arena_idx);
    background_thread_lock.unlock(&thread_state);

    return result;
}

/// jemalloc: background_threads_enable
bool backgroundThreadsEnable(ThreadState & thread_state)
{
    ALLOCATOR_ASSERT(num_background_threads == 0);
    ALLOCATOR_ASSERT(backgroundThreadEnabled());
    background_thread_lock.assertOwner(&thread_state);

    /// jemalloc uses a variable-length array of `max_background_threads` (at most `MAX_BACKGROUND_THREAD_LIMIT`).
    std::array<bool, MAX_BACKGROUND_THREAD_LIMIT> marked;
    unsigned num_marked;
    for (size_t i = 0; i < max_background_threads; ++i)
        marked[i] = false;
    num_marked = 0;
    /// Thread 0 is required and created at the end.
    marked[0] = true;
    /// Mark the threads we need to create for thread 0.
    unsigned num_arenas = numArenasTotalGet();
    for (unsigned i = 1; i < num_arenas; ++i)
    {
        if (marked[i % max_background_threads] || arenaGet(&thread_state, i, false) == nullptr)
            continue;
        BackgroundThreadInfo * info = &background_thread_info[i % max_background_threads];
        info->mutex.lock(&thread_state);
        ALLOCATOR_ASSERT(info->state == BackgroundThreadState::Stopped);
        backgroundThreadInit(thread_state, info);
        info->mutex.unlock(&thread_state);
        marked[i % max_background_threads] = true;
        if (++num_marked == max_background_threads)
            break;
    }

    bool error = backgroundThreadCreateLocked(thread_state, 0);
    if (error)
        return true;
    for (unsigned i = 0; i < num_arenas; ++i)
    {
        Arena * arena = arenaGet(&thread_state, i, false);
        if (arena != nullptr)
            arena->page_allocator_shard.setDeferralAllowed(&thread_state, true);
    }
    return false;
}

/// jemalloc: background_threads_disable
bool backgroundThreadsDisable(ThreadState & thread_state)
{
    ALLOCATOR_ASSERT(!backgroundThreadEnabled());
    background_thread_lock.assertOwner(&thread_state);

    /// Thread 0 will be responsible for terminating other threads.
    if (backgroundThreadsDisableSingle(thread_state, &background_thread_info[0]))
        return true;
    ALLOCATOR_ASSERT(num_background_threads == 0);
    unsigned num_arenas = numArenasTotalGet();
    for (unsigned i = 0; i < num_arenas; ++i)
    {
        Arena * arena = arenaGet(&thread_state, i, false);
        if (arena != nullptr)
            arena->page_allocator_shard.setDeferralAllowed(&thread_state, false);
    }

    return false;
}

/// jemalloc: background_thread_is_started
bool backgroundThreadIsStarted(BackgroundThreadInfo * info)
{
    return info->state == BackgroundThreadState::Started;
}

/// jemalloc: background_thread_wakeup_early
void backgroundThreadWakeupEarly(BackgroundThreadInfo * info, Nanoseconds * remaining_sleep)
{
    /// This is an optimization to increase batching. At this point we know that background thread wakes up soon, so
    /// the time to cache the just freed memory is bounded and low.
    if (remaining_sleep != nullptr && remaining_sleep->ns() < BACKGROUND_THREAD_MIN_INTERVAL_NS)
        return;
    pthread_cond_signal(&info->condition);
}

/// jemalloc: background_thread_prefork0
void backgroundThreadPrefork0(ThreadState * thread_state)
{
    background_thread_lock.prefork(thread_state);
    background_thread_enabled_at_fork = backgroundThreadEnabled();
}

/// jemalloc: background_thread_prefork1
void backgroundThreadPrefork1(ThreadState * thread_state)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mutex.prefork(thread_state);
}

/// jemalloc: background_thread_postfork_parent
void backgroundThreadPostforkParent(ThreadState * thread_state)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mutex.postforkParent(thread_state);
    background_thread_lock.postforkParent(thread_state);
}

/// jemalloc: background_thread_postfork_child
void backgroundThreadPostforkChild(ThreadState * thread_state)
{
    for (unsigned i = 0; i < max_background_threads; ++i)
        background_thread_info[i].mutex.postforkChild(thread_state);
    background_thread_lock.postforkChild(thread_state);
    if (!background_thread_enabled_at_fork)
        return;

    /// Clear background_thread state (reset to disabled for child).
    background_thread_lock.lock(thread_state);
    num_background_threads = 0;
    backgroundThreadEnabledSet(thread_state, false);
    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        info->mutex.lock(thread_state);
        info->state = BackgroundThreadState::Stopped;
        [[maybe_unused]] int result = conditionInit(&info->condition, /* fallback_to_default */ true);
        ALLOCATOR_ASSERT(result == 0);
        backgroundThreadInfoInit(thread_state, info);
        info->mutex.unlock(thread_state);
    }
    background_thread_lock.unlock(thread_state);
}

/// jemalloc: background_thread_stats_read
bool backgroundThreadStatsRead(ThreadState * thread_state, BackgroundThreadStats * stats)
{
    static_assert(config::stats);
    background_thread_lock.lock(thread_state);
    if (!backgroundThreadEnabled())
    {
        background_thread_lock.unlock(thread_state);
        return true;
    }

    stats->run_interval.initZero();
    stats->max_counter_per_background_thread.reset();

    uint64_t num_runs = 0;
    stats->num_threads = num_background_threads;
    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = &background_thread_info[i];
        if (!info->mutex.tryLock(thread_state))
        {
            /// Each background thread run may take a long time; avoid waiting on the stats if the thread is active.
            continue;
        }
        if (info->state != BackgroundThreadState::Stopped)
        {
            num_runs += info->total_num_runs;
            stats->run_interval.add(info->total_sleep_time);
            info->mutex.profilingMaxUpdate(thread_state, stats->max_counter_per_background_thread);
        }
        info->mutex.unlock(thread_state);
    }
    stats->num_runs = num_runs;
    if (num_runs > 0)
        stats->run_interval.divideBy(num_runs);
    background_thread_lock.unlock(thread_state);

    return false;
}

/// When lazy lock is enabled, we need to make sure setting isthreaded before taking any background_thread locks. This
/// is called early in ctl (instead of wait for the pthread_create calls to trigger) because the mutex is required
/// before creating background threads.
/// jemalloc: background_thread_ctl_init
void backgroundThreadMallctlInit(ThreadState * thread_state)
{
    background_thread_lock.assertNotOwner(thread_state);
    pthreadCreateFunctionPtrInit();
    pthreadCreateWrapperInit();
}

/// jemalloc: pthread_create_wrapper
int pthreadCreateWrapper(pthread_t * thread, const pthread_attr_t * attributes, void * (*start_routine)(void *), void * arg)
{
    pthreadCreateWrapperInit();

    return pthread_create_function_ptr(thread, attributes, start_routine, arg);
}

/// jemalloc: background_thread_boot0
bool backgroundThreadBoot0()
{
    /// `!have_background_thread && opt_background_thread` ("option background_thread currently supports pthread
    /// only") cannot happen: background threads are supported on all platforms.
    static_assert(config::background_thread);
    /// `JEMALLOC_PTHREAD_CREATE_WRAPPER` is defined everywhere (`JEMALLOC_BACKGROUND_THREAD`).
    if ((config::lazy_lock || options.background_thread) && pthreadCreateFunctionPtrInit())
        return true;
    return false;
}

/// jemalloc: background_thread_boot1
bool backgroundThreadBoot1(ThreadState * thread_state, Base * base)
{
    ALLOCATOR_ASSERT(numArenasTotalGet() > 0);

    if (options.max_background_threads > MAX_BACKGROUND_THREAD_LIMIT)
        options.max_background_threads = DEFAULT_NUM_BACKGROUND_THREAD;
    max_background_threads = options.max_background_threads;

    if (background_thread_lock.init("background_thread_global", MutexRank::BACKGROUND_THREAD_GLOBAL, MutexLockOrder::RankExclusive))
        return true;

    background_thread_info = static_cast<BackgroundThreadInfo *>(
        base->alloc(thread_state, options.max_background_threads * sizeof(BackgroundThreadInfo), CACHE_LINE));
    if (background_thread_info == nullptr)
        return true;

    for (unsigned i = 0; i < max_background_threads; ++i)
    {
        BackgroundThreadInfo * info = new (&background_thread_info[i]) BackgroundThreadInfo;
        /// Thread mutex is rank_inclusive because of thread0.
        if (info->mutex.init("background_thread", MutexRank::BACKGROUND_THREAD, MutexLockOrder::AddressOrdered))
            return true;
        if (conditionInit(&info->condition, /* fallback_to_default */ false))
            return true;
        info->mutex.lock(thread_state);
        info->state = BackgroundThreadState::Stopped;
        backgroundThreadInfoInit(thread_state, info);
        info->mutex.unlock(thread_state);
    }
    /// Using `Impl` to bypass the locking check during init.
    backgroundThreadEnabledSetImpl(options.background_thread);
    return false;
}

/// --- Hooks of the arena module (declared in Arena.h) ---------------------------------------------------------------

/// jemalloc: arena_background_thread_info_get
BackgroundThreadInfo * arenaBackgroundThreadInfoGet(Arena * arena)
{
    unsigned arena_idx = arenaIdxGet(arena);
    return &background_thread_info[arena_idx % max_background_threads];
}

/// `&info->mutex`
Mutex & backgroundThreadInfoMutex(BackgroundThreadInfo * info)
{
    return info->mutex;
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

/// `info->num_pages_to_purge_new`
size_t & backgroundThreadNumPagesToPurgeNew(BackgroundThreadInfo * info)
{
    return info->num_pages_to_purge_new;
}

}

#if defined(__FreeBSD__)
static_assert(jemalloc::config::lazy_lock);
/// We intercept `pthread_create` calls in order to toggle `is_threaded` if the process goes multi-threaded
/// (`JEMALLOC_LAZY_LOCK`). jemalloc: pthread_create (`src/mutex.c`)
extern "C" __attribute__((visibility("default"))) int pthread_create(
    pthread_t * __restrict thread, const pthread_attr_t * __restrict attributes, void * (*start_routine)(void *), void * __restrict arg)
{
    return jemalloc::pthreadCreateWrapper(thread, attributes, start_routine, arg);
}
#else
static_assert(!jemalloc::config::lazy_lock);
#endif
