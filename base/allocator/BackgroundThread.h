#pragma once

/// Background threads that purge dirty pages of the arenas according to the decay schedule.
/// jemalloc: `background_thread_structs.h`, `background_thread_externs.h`, `background_thread_inlines.h`,
/// `src/background_thread.c` (with the ClickHouse fork patches 86a24dd0, f3365c8c, 861db0b4: the condition variable
/// uses `CLOCK_MONOTONIC` where the clock is monotonic; 4d0ffa07: only arena 0 creates thread 0; 86bbabac: the
/// `pthread_create` lookup falls back to `RTLD_DEFAULT` and to the linked symbol).
///
/// Thread `k` of `max_background_threads` serves the arenas `k, k + max, k + 2 * max, ...`. Thread 0 is created
/// synchronously (by the initialization or by `background_thread` mallctl); it creates the other threads on demand
/// and stops them when background threads are disabled.

#include <allocator/Common.h>
#include <allocator/Mutex.h>
#include <allocator/Nanoseconds.h>
#include <allocator/Options.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <pthread.h>

namespace jemalloc
{

class Arena;
class Base;
class ThreadState;

/// --- Constants (background_thread_structs.h, background_thread.c) --------------------------------------------------

/// jemalloc: BACKGROUND_THREAD_INDEFINITE_SLEEP
inline constexpr uint64_t BACKGROUND_THREAD_INDEFINITE_SLEEP = UINT64_MAX;

/// These exist only as a transitional state (deferral should be part of the page allocator interface).
/// jemalloc: BACKGROUND_THREAD_DEFERRED_MIN, BACKGROUND_THREAD_DEFERRED_MAX
inline constexpr uint64_t BACKGROUND_THREAD_DEFERRED_MIN = 0;
inline constexpr uint64_t BACKGROUND_THREAD_DEFERRED_MAX = UINT64_MAX;

/// Minimal sleep interval: 100 ms. jemalloc: BACKGROUND_THREAD_MIN_INTERVAL_NS (`background_thread.c`)
inline constexpr uint64_t BACKGROUND_THREAD_MIN_INTERVAL_NS = Nanoseconds::BILLION / 10;

/// `MAX_BACKGROUND_THREAD_LIMIT` and `DEFAULT_NUM_BACKGROUND_THREAD` are in Options.h (they define the option default).

/// --- Data structures -----------------------------------------------------------------------------------------------

/// jemalloc: background_thread_state_t
enum class BackgroundThreadState : int
{
    /// jemalloc: background_thread_stopped
    Stopped,
    /// jemalloc: background_thread_started
    Started,
    /// The thread waits on the global lock when paused (for `arena_reset`). jemalloc: background_thread_paused
    Paused,
};

/// Allocated from `b0` by `backgroundThreadBoot1` (the size is observable through `stats.metadata`).
/// jemalloc: background_thread_info_t
struct BackgroundThreadInfo
{
    pthread_t thread;
    /// `CLOCK_MONOTONIC` where `config::have_clock_monotonic` (the clock of `Nanoseconds`), `CLOCK_REALTIME` on Darwin.
    pthread_cond_t condition;
    /// "background_thread", `MutexRank::BACKGROUND_THREAD`, address ordered (thread 0 locks the others).
    Mutex mutex;
    /// Protected by `mutex`.
    BackgroundThreadState state;
    /// When true, it means no wakeup scheduled.
    std::atomic<bool> indefinite_sleep;
    /// Next scheduled wakeup time (absolute time in ns).
    Nanoseconds next_wakeup;
    /// Since the last background thread run, newly added number of pages that need to be purged by the next wakeup.
    /// This is adjusted on epoch advance, and is used to determine whether we should signal the background thread to
    /// wake up earlier.
    size_t num_pages_to_purge_new;
    /// Stats: total number of runs since started.
    uint64_t total_num_runs;
    /// Stats: total sleep time since started.
    Nanoseconds total_sleep_time;

    /// jemalloc: background_thread_wakeup_time_get
    ALLOCATOR_ALWAYS_INLINE uint64_t wakeupTimeGet() const
    {
        uint64_t result = next_wakeup.ns();
        ALLOCATOR_ASSERT(indefinite_sleep.load(std::memory_order_acquire) == (result == BACKGROUND_THREAD_INDEFINITE_SLEEP));
        return result;
    }

    /// Requires `mutex`. jemalloc: background_thread_wakeup_time_set
    ALLOCATOR_ALWAYS_INLINE void wakeupTimeSet(ThreadState * thread_state, uint64_t wakeup_time)
    {
        mutex.assertOwner(thread_state);
        indefinite_sleep.store(wakeup_time == BACKGROUND_THREAD_INDEFINITE_SLEEP, std::memory_order_release);
        next_wakeup.init(wakeup_time);
    }

    /// jemalloc: background_thread_indefinite_sleep
    ALLOCATOR_ALWAYS_INLINE bool indefiniteSleep() const { return indefinite_sleep.load(std::memory_order_acquire); }
};

/// The same layout as `background_thread_info_t` (verified by background_thread_oracle).
static_assert(
    sizeof(BackgroundThreadInfo)
    == alignmentCeiling(sizeof(pthread_t) + sizeof(pthread_cond_t) + sizeof(Mutex) + sizeof(int) + 1, 8) + 4 * 8);
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(BackgroundThreadInfo) == 216, "background_thread_info_t is 216 bytes on aarch64 glibc");
#elif defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
static_assert(sizeof(BackgroundThreadInfo) == 208, "background_thread_info_t is 208 bytes on x86_64 glibc");
#endif

/// The value of `stats.background_thread.*` and `stats.mutexes.max_per_bg_thd.*`.
/// jemalloc: background_thread_stats_t
struct BackgroundThreadStats
{
    size_t num_threads;
    uint64_t num_runs;
    Nanoseconds run_interval;
    MutexProfilingData max_counter_per_background_thread;
};

static_assert(sizeof(BackgroundThreadStats) == 88, "Must have the size of background_thread_stats_t");

/// --- Globals (background_thread.c) ---------------------------------------------------------------------------------

/// Used for thread creation, termination and stats. "background_thread_global",
/// `MutexRank::BACKGROUND_THREAD_GLOBAL`. jemalloc: background_thread_lock
extern constinit Mutex background_thread_lock;
/// Indicates global state. Atomic because decay reads this without locking.
/// jemalloc: background_thread_enabled_state
extern constinit std::atomic<bool> background_thread_enabled_state;
/// Protected by `background_thread_lock` (thread 0 modifies it while the ctl thread waits for it holding the lock).
/// jemalloc: n_background_threads
extern constinit size_t num_background_threads;
/// jemalloc: max_background_threads
extern constinit size_t max_background_threads;
/// Thread info per index: an array of `opt.max_background_threads` elements. jemalloc: background_thread_info
extern constinit BackgroundThreadInfo * background_thread_info;

/// --- Inlines (background_thread_inlines.h) -------------------------------------------------------------------------

/// jemalloc: background_thread_enabled
ALLOCATOR_ALWAYS_INLINE bool backgroundThreadEnabled()
{
    return background_thread_enabled_state.load(std::memory_order_relaxed);
}

/// jemalloc: background_thread_enabled_set_impl
ALLOCATOR_ALWAYS_INLINE void backgroundThreadEnabledSetImpl(bool state)
{
    background_thread_enabled_state.store(state, std::memory_order_relaxed);
}

/// Requires `background_thread_lock`. jemalloc: background_thread_enabled_set
ALLOCATOR_ALWAYS_INLINE void backgroundThreadEnabledSet(ThreadState * thread_state, bool state)
{
    background_thread_lock.assertOwner(thread_state);
    backgroundThreadEnabledSetImpl(state);
}

/// jemalloc: background_thread_info_get
ALLOCATOR_ALWAYS_INLINE BackgroundThreadInfo * backgroundThreadInfoGet(size_t idx)
{
    return &background_thread_info[idx % max_background_threads];
}

/// --- Functions (background_thread_externs.h) -----------------------------------------------------------------------
/// Functions returning `bool` return true on error.

/// Create a new background thread if needed (for arena `arena_idx != 0` it is created asynchronously by thread 0).
/// jemalloc: background_thread_create
bool backgroundThreadCreate(ThreadState & thread_state, unsigned arena_idx);

/// Requires `background_thread_lock` and `backgroundThreadEnabled()`. jemalloc: background_threads_enable
bool backgroundThreadsEnable(ThreadState & thread_state);

/// Stops and joins all threads (thread 0 stops the others). Requires `background_thread_lock` and
/// `!backgroundThreadEnabled()`. jemalloc: background_threads_disable
bool backgroundThreadsDisable(ThreadState & thread_state);

/// Also declared in Arena.h (the arena's deferred work hooks).
/// jemalloc: background_thread_is_started
bool backgroundThreadIsStarted(BackgroundThreadInfo * info);

/// Signals the thread unless it is going to wake up within the minimal interval anyway.
/// jemalloc: background_thread_wakeup_early
void backgroundThreadWakeupEarly(BackgroundThreadInfo * info, Nanoseconds * remaining_sleep);

/// jemalloc: background_thread_prefork0, background_thread_prefork1, background_thread_postfork_parent,
/// background_thread_postfork_child
void backgroundThreadPrefork0(ThreadState * thread_state);
void backgroundThreadPrefork1(ThreadState * thread_state);
void backgroundThreadPostforkParent(ThreadState * thread_state);
/// Background threads are disabled in the child (the threads do not exist there).
void backgroundThreadPostforkChild(ThreadState * thread_state);

/// Returns true (and leaves `stats` untouched) if background threads are disabled.
/// jemalloc: background_thread_stats_read
bool backgroundThreadStatsRead(ThreadState * thread_state, BackgroundThreadStats * stats);

/// Must be called before taking `background_thread_lock` in ctl (sets `is_threaded` with lazy locking).
/// jemalloc: background_thread_ctl_init
void backgroundThreadMallctlInit(ThreadState * thread_state);

/// Calls the real `pthread_create` (resolved with `dlsym`); sets `is_threaded` with lazy locking.
/// jemalloc: pthread_create_wrapper
int pthreadCreateWrapper(pthread_t * thread, const pthread_attr_t * attributes, void * (*start_routine)(void *), void * arg);

/// During `malloc_init_a0` (after the options are parsed). jemalloc: background_thread_boot0
bool backgroundThreadBoot0();

/// From `malloc_init_hard` after `malloc_init_narenas`. Replaces `opt.max_background_threads` above
/// `MAX_BACKGROUND_THREAD_LIMIT` (the default) with `DEFAULT_NUM_BACKGROUND_THREAD` and allocates the infos from
/// `base`. jemalloc: background_thread_boot1
bool backgroundThreadBoot1(ThreadState * thread_state, Base * base);

/// --- The sleep interval computation of a work pass (exposed for testing) ----------------------------------------

/// The interval to sleep given the soonest deferred work time over the served arenas.
/// jemalloc: the end of background_work_sleep_once
constexpr uint64_t backgroundThreadSleepInterval(uint64_t ns_until_deferred)
{
    if (ns_until_deferred == BACKGROUND_THREAD_DEFERRED_MAX)
        return BACKGROUND_THREAD_INDEFINITE_SLEEP;
    return ns_until_deferred < BACKGROUND_THREAD_MIN_INTERVAL_NS ? BACKGROUND_THREAD_MIN_INTERVAL_NS : ns_until_deferred;
}

/// One pass over the arenas `idx, idx + stride, ...` below `num_arenas`: `ops.get(i)` returns the arena (or null),
/// `ops.doWork(arena)` does the deferred work (skipped after an indefinite sleep: the wakeup only reschedules),
/// `ops.timeUntilDeferredWork(arena)` is queried until the minimum drops to the minimal interval. Returns the
/// interval to sleep. jemalloc: background_work_sleep_once (without the sleep)
template <typename Ops>
uint64_t backgroundWorkPass(unsigned idx, unsigned num_arenas, size_t stride, bool slept_indefinitely, Ops && ops)
{
    uint64_t ns_until_deferred = BACKGROUND_THREAD_DEFERRED_MAX;
    for (unsigned i = idx; i < num_arenas; i += static_cast<unsigned>(stride))
    {
        auto * arena = ops.get(i);
        if (!arena)
            continue;
        /// If the thread was woken up from the indefinite sleep, don't do the work instantly, but rather check when
        /// the deferred work that caused this thread to wake up is scheduled for.
        if (!slept_indefinitely)
            ops.doWork(arena);
        if (ns_until_deferred <= BACKGROUND_THREAD_MIN_INTERVAL_NS)
        {
            /// Min interval will be used.
            continue;
        }
        uint64_t ns_arena_deferred = ops.timeUntilDeferredWork(arena);
        if (ns_arena_deferred < ns_until_deferred)
            ns_until_deferred = ns_arena_deferred;
    }
    return backgroundThreadSleepInterval(ns_until_deferred);
}

}
