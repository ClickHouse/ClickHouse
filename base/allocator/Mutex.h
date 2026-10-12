#pragma once

/// The allocator's mutex: a `pthread_mutex_t` with spin-then-block locking and contention profiling.
/// jemalloc: `mutex.h`, `src/mutex.c`, `mutex_prof.h`.
///
/// `malloc_mutex_t` is `pthread_mutex_t` on all ClickHouse platforms, including Darwin (the fork leaves
/// `JEMALLOC_OS_UNFAIR_LOCK` undefined so that `pthread_cond_wait` works with it). Witness (lock-order checking) is not
/// implemented; `MutexRank` documents the ranks for a future debug checker. The layout is that of `malloc_mutex_t`
/// without `JEMALLOC_DEBUG` (where the witness is in a union with the other fields and costs no memory), because its
/// size is observable through `stats.metadata` (it is embedded in arenas, bins, ...).

#include <allocator/Common.h>
#include <allocator/Nanoseconds.h>
#include <allocator/Options.h>

#include <atomic>
#include <cstdint>
#include <type_traits>
#include <pthread.h>

namespace jemalloc
{

class ThreadState;

/// The number of CPUs, set during initialization (`malloc_ncpus`); 0 before that.
/// jemalloc: ncpus (`src/jemalloc.c`). Defined in Init.cpp.
extern unsigned num_cpus;

/// With `JEMALLOC_LAZY_LOCK` (FreeBSD) mutexes are not locked until the process goes multi-threaded: the
/// `pthread_create` wrapper sets this. Elsewhere `isThreaded()` is the constant true.
/// jemalloc: isthreaded
extern bool is_threaded;

ALLOCATOR_ALWAYS_INLINE bool isThreaded()
{
    if constexpr (config::lazy_lock)
        return is_threaded;
    else
        return true;
}

/// FreeBSD (`JEMALLOC_MUTEX_INIT_CB`): the `calloc` used by libc to allocate the internals of `pthread_mutex_t`
/// (allocates from arena 0, `arena0InternalAllocate(num * size, zero = true, is_internal = false)`); defined by the initialization
/// code.
/// jemalloc: bootstrap_calloc
void * bootstrapAllocateZeroed(size_t num, size_t size);

/// Lock ranks (jemalloc: `witness_rank_t`, `witness.h`). A thread may only acquire a mutex with a rank strictly
/// greater than every held one (or equal for `AddressOrdered` mutexes in ascending address order).
/// Documentation only: there is no witness checking.
enum class MutexRank : unsigned
{
    /// Ignored by the witness machinery.
    OMIT,
    MIN,
    INIT = MIN,
    MALLCTL,
    EXPLICIT_THREAD_CACHES,
    ARENAS,
    BACKGROUND_THREAD_GLOBAL,
    PROFILING_DUMP,
    PROFILING_BACKTRACE_TO_GLOBAL_CONTEXT,
    PROFILING_ALL_THREAD_DATA,
    PROFILING_THREAD_DATA,
    PROFILING_LOG,
    PROFILING_GLOBAL_CONTEXT,
    PROFILING_RECENT_DUMP,
    BACKGROUND_THREAD,
    /// The minimally ranked core lock.
    CORE,
    DECAY = CORE,
    THREAD_CACHE_LIST,
    SMALL_EXTENT_CACHE_BIN,
    EXTENT_GROW,
    HUGE_PAGE_SHARD_GROW = EXTENT_GROW,
    SANITIZER_BUMP_ALLOC = EXTENT_GROW,
    EXTENTS,
    HUGE_PAGE_SHARD = EXTENTS,
    HUGE_PAGE_ALLOCATOR_CENTRAL_GROW,
    HUGE_PAGE_ALLOCATOR_CENTRAL,
    EXTENT_POOL,
    RADIX_TREE,
    BASE,
    ARENA_LARGE,
    HOOK,

    LEAF = 0x1000,
    BIN = LEAF,
    ARENA_STATS = LEAF,
    COUNTER_ACCUMULATED = LEAF,
    SBRK = LEAF,
    PROFILING_ACTIVE = LEAF,
    PROFILING_DUMP_FILENAME = LEAF,
    PROFILING_GROWTH_DUMP = LEAF,
    PROFILING_NEXT_THREAD_UID = LEAF,
    PROFILING_RECENT_ALLOC = LEAF,
    PROFILING_STATS = LEAF,
    PROFILING_THREAD_ACTIVE_INIT = LEAF,
    THREAD_EVENTS_USER = LEAF,
};

/// jemalloc: malloc_mutex_lock_order_t
enum class MutexLockOrder : unsigned
{
    /// Can only acquire one mutex of a given rank at a time.
    RankExclusive,
    /// Can acquire multiple mutexes of the same rank, but in address-ascending order only.
    AddressOrdered,
};

/// --- Mutex profiling (mutex_prof.h) -----------------------------------------------------------------------------

/// The global mutexes reported in `stats.mutexes.*`, in this order.
/// jemalloc: mutex_prof_global_ind_t (MUTEX_PROF_GLOBAL_MUTEXES)
enum MutexProfilingGlobalIdx : unsigned
{
    global_profiling_mutex_background_thread,
    global_profiling_mutex_max_per_background_thread,
    global_profiling_mutex_mallctl,
    global_profiling_mutex_profiling,
    global_profiling_mutex_profiling_threads_data,
    global_profiling_mutex_profiling_dump,
    global_profiling_mutex_profiling_recent_alloc,
    global_profiling_mutex_profiling_recent_dump,
    global_profiling_mutex_profiling_stats,
    mutex_profiling_num_global_mutexes,
};

inline constexpr const char * mutex_profiling_global_names[mutex_profiling_num_global_mutexes] = {
    "background_thread",
    "max_per_bg_thd",
    "ctl",
    "prof",
    "prof_thds_data",
    "prof_dump",
    "prof_recent_alloc",
    "prof_recent_dump",
    "prof_stats",
};

/// The per-arena mutexes reported in `stats.arenas.<i>.mutexes.*`, in this order.
/// jemalloc: mutex_prof_arena_ind_t (MUTEX_PROF_ARENA_MUTEXES)
enum MutexProfilingArenaIdx : unsigned
{
    arena_profiling_mutex_large,
    arena_profiling_mutex_extent_available,
    arena_profiling_mutex_extents_dirty,
    arena_profiling_mutex_extents_muzzy,
    arena_profiling_mutex_extents_retained,
    arena_profiling_mutex_decay_dirty,
    arena_profiling_mutex_decay_muzzy,
    arena_profiling_mutex_base,
    arena_profiling_mutex_thread_cache_list,
    arena_profiling_mutex_huge_page_shard,
    arena_profiling_mutex_huge_page_shard_grow,
    arena_profiling_mutex_small_extent_cache,
    mutex_profiling_num_arena_mutexes,
};

inline constexpr const char * mutex_profiling_arena_names[mutex_profiling_num_arena_mutexes] = {
    "large",
    "extent_avail",
    "extents_dirty",
    "extents_muzzy",
    "extents_retained",
    "decay_dirty",
    "decay_muzzy",
    "base",
    "tcache_list",
    "hpa_shard",
    "hpa_shard_grow",
    "hpa_sec",
};

/// The counters of the mutex statistics (columns of the stats tables and leaves of the mallctl tree).
/// `derived` counters are rates (`(#/seconds)`) computed from `base_counter`.
/// jemalloc: MUTEX_PROF_UINT64_COUNTERS, MUTEX_PROF_UINT32_COUNTERS
enum MutexProfilingUint64CounterIdx : unsigned
{
    mutex_counter_num_ops,
    mutex_counter_num_ops_per_second,
    mutex_counter_num_wait,
    mutex_counter_num_wait_per_second,
    mutex_counter_num_spin_acquired,
    mutex_counter_num_spin_acquired_per_second,
    mutex_counter_num_owner_switch,
    mutex_counter_num_owner_switch_per_second,
    mutex_counter_total_wait_time,
    mutex_counter_total_wait_time_per_second,
    mutex_counter_max_wait_time,
    mutex_profiling_num_uint64_t_counters,
};

enum MutexProfilingUint32CounterIdx : unsigned
{
    mutex_counter_max_num_threads,
    mutex_profiling_num_uint32_t_counters,
};

struct MutexProfilingCounterInfo
{
    const char * name;
    const char * human;
    bool derived;
    unsigned base_counter;
};

inline constexpr MutexProfilingCounterInfo mutex_profiling_uint64_counters[mutex_profiling_num_uint64_t_counters] = {
    {"num_ops", "n_lock_ops", false, mutex_counter_num_ops},
    {"num_ops_ps", "(#/sec)", true, mutex_counter_num_ops},
    {"num_wait", "n_waiting", false, mutex_counter_num_wait},
    {"num_wait_ps", "(#/sec)", true, mutex_counter_num_wait},
    {"num_spin_acq", "n_spin_acq", false, mutex_counter_num_spin_acquired},
    {"num_spin_acq_ps", "(#/sec)", true, mutex_counter_num_spin_acquired},
    {"num_owner_switch", "n_owner_switch", false, mutex_counter_num_owner_switch},
    {"num_owner_switch_ps", "(#/sec)", true, mutex_counter_num_owner_switch},
    {"total_wait_time", "total_wait_ns", false, mutex_counter_total_wait_time},
    {"total_wait_time_ps", "(#/sec)", true, mutex_counter_total_wait_time},
    {"max_wait_time", "max_wait_ns", false, mutex_counter_max_wait_time},
};

inline constexpr MutexProfilingCounterInfo mutex_profiling_uint32_counters[mutex_profiling_num_uint32_t_counters] = {
    {"max_num_thds", "max_n_thds", false, mutex_counter_max_num_threads},
};

/// jemalloc: mutex_prof_data_t
///
/// Zero-filled memory is a valid initial state (like `LOCK_PROF_DATA_INITIALIZER`).
struct MutexProfilingData
{
    /// Counters touched on the slow path, i.e. when there is lock contention. Updated once we have the lock.

    /// Total time spent waiting on this mutex.
    Nanoseconds total_wait_time = Nanoseconds::zero();
    /// Max time spent on a single lock operation.
    Nanoseconds max_wait_time = Nanoseconds::zero();
    /// # of times have to wait for this mutex (after spinning).
    uint64_t num_wait_times = 0;
    /// # of times acquired the mutex through local spinning.
    uint64_t num_spin_acquired = 0;
    /// Max # of threads waiting for the mutex at the same time.
    uint32_t max_num_threads = 0;
    /// Current # of threads waiting on the lock (modified without holding the lock).
    std::atomic<uint32_t> num_waiting_threads{0};

    /// Data touched on the fast path, right after acquiring the lock (placed right before the lock to share its
    /// cache line).

    /// # of times the mutex holder is different than the previous one.
    uint64_t num_owner_switches = 0;
    /// Previous mutex holder, to facilitate n_owner_switches.
    ThreadState * prev_owner = nullptr;
    /// # of lock() operations in total.
    uint64_t num_lock_ops = 0;

    constexpr MutexProfilingData() = default;
    MutexProfilingData(const MutexProfilingData &) = delete;
    MutexProfilingData & operator=(const MutexProfilingData &) = delete;

    /// jemalloc: mutex_prof_data_init
    void reset()
    {
        total_wait_time.initZero();
        max_wait_time.initZero();
        num_wait_times = 0;
        num_spin_acquired = 0;
        max_num_threads = 0;
        num_waiting_threads.store(0, std::memory_order_relaxed);
        num_owner_switches = 0;
        prev_owner = nullptr;
        num_lock_ops = 0;
    }

    /// A member-for-member copy (including `prev_owner`), except `num_waiting_threads` which is not reported and is zeroed.
    /// jemalloc: malloc_mutex_prof_copy
    void copyFrom(const MutexProfilingData & source)
    {
        total_wait_time = source.total_wait_time;
        max_wait_time = source.max_wait_time;
        num_wait_times = source.num_wait_times;
        num_spin_acquired = source.num_spin_acquired;
        max_num_threads = source.max_num_threads;
        num_owner_switches = source.num_owner_switches;
        prev_owner = source.prev_owner;
        num_lock_ops = source.num_lock_ops;
        num_waiting_threads.store(0, std::memory_order_relaxed);
    }

    /// Aggregate (this is the sum).
    /// jemalloc: malloc_mutex_prof_merge
    void merge(const MutexProfilingData & data)
    {
        total_wait_time.add(data.total_wait_time);
        if (max_wait_time.compare(data.max_wait_time) < 0)
            max_wait_time.copy(data.max_wait_time);
        num_wait_times += data.num_wait_times;
        num_spin_acquired += data.num_spin_acquired;
        if (max_num_threads < data.max_num_threads)
            max_num_threads = data.max_num_threads;
        uint32_t current_num_waiting_threads = num_waiting_threads.load(std::memory_order_relaxed);
        uint32_t new_num_waiting_threads = current_num_waiting_threads + data.num_waiting_threads.load(std::memory_order_relaxed);
        num_waiting_threads.store(new_num_waiting_threads, std::memory_order_relaxed);
        num_owner_switches += data.num_owner_switches;
        num_lock_ops += data.num_lock_ops;
    }
};

static_assert(sizeof(MutexProfilingData) == 64, "Must have the size of mutex_prof_data_t");

/// jemalloc: malloc_mutex_t
class Mutex
{
public:
    /// A statically initialized mutex.
    /// jemalloc: MALLOC_MUTEX_INITIALIZER
    constexpr Mutex() = default;

    Mutex(const Mutex &) = delete;
    Mutex & operator=(const Mutex &) = delete;

    /// `name` and `rank` are kept only for documentation (jemalloc stores them in the witness, debug builds only).
    /// Returns true on error.
    /// jemalloc: malloc_mutex_init
    bool init(const char * name, MutexRank rank, MutexLockOrder lock_order = MutexLockOrder::RankExclusive);

    /// jemalloc: malloc_mutex_lock
    ALLOCATOR_ALWAYS_INLINE void lock(ThreadState * thread_state)
    {
        if (isThreaded())
        {
            if (tryLockFinal())
                lockSlow();
            ALLOCATOR_ASSERT(isLocked());
            ownerStatsUpdate(thread_state);
        }
    }

    /// Returns true if the lock is acquired (note: jemalloc's `malloc_mutex_trylock` returns true on failure).
    /// jemalloc: malloc_mutex_trylock
    ALLOCATOR_ALWAYS_INLINE bool tryLock(ThreadState * thread_state)
    {
        if (isThreaded())
        {
            if (tryLockFinal())
                return false;
            ALLOCATOR_ASSERT(isLocked());
            ownerStatsUpdate(thread_state);
        }
        return true;
    }

    /// jemalloc: malloc_mutex_unlock
    ALLOCATOR_ALWAYS_INLINE void unlock(ThreadState * /*tsdn*/)
    {
        if (isThreaded())
        {
            ALLOCATOR_ASSERT(isLocked());
            locked.store(false, std::memory_order_relaxed);
            pthread_mutex_unlock(&lock_);
        }
    }

    /// For sanity checking only: whether some thread holds the lock.
    /// jemalloc: malloc_mutex_is_locked
    ALLOCATOR_ALWAYS_INLINE bool isLocked() const { return locked.load(std::memory_order_relaxed); }

    /// Without witness, only checks that the mutex is locked (by somebody).
    /// jemalloc: malloc_mutex_assert_owner
    ALLOCATOR_ALWAYS_INLINE void assertOwner(ThreadState * /*tsdn*/) const
    {
        if (isThreaded())
            ALLOCATOR_ASSERT(isLocked());
    }

    /// A no-op without witness.
    /// jemalloc: malloc_mutex_assert_not_owner
    ALLOCATOR_ALWAYS_INLINE void assertNotOwner(ThreadState * /*tsdn*/) const { }

    /// jemalloc: malloc_mutex_prefork
    void prefork(ThreadState * thread_state);
    /// jemalloc: malloc_mutex_postfork_parent
    void postforkParent(ThreadState * thread_state);
    /// Re-initializes the mutex (just unlocks it with `JEMALLOC_MUTEX_INIT_CB`).
    /// jemalloc: malloc_mutex_postfork_child
    void postforkChild(ThreadState * thread_state);

    /// Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_data_reset
    void profilingDataReset(ThreadState * thread_state);

    /// Copy the prof data for processing. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_read
    ALLOCATOR_ALWAYS_INLINE void profilingRead(ThreadState * thread_state, MutexProfilingData & data)
    {
        assertOwner(thread_state);
        data.copyFrom(profiling_data);
    }

    /// Accumulate the prof data into `data`. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_accum
    ALLOCATOR_ALWAYS_INLINE void profilingAccumulated(ThreadState * thread_state, MutexProfilingData & data)
    {
        const MutexProfilingData & source = profiling_data;
        assertOwner(thread_state);
        data.total_wait_time.add(source.total_wait_time);
        if (source.max_wait_time.compare(data.max_wait_time) > 0)
            data.max_wait_time.copy(source.max_wait_time);
        data.num_wait_times += source.num_wait_times;
        data.num_spin_acquired += source.num_spin_acquired;
        if (data.max_num_threads < source.max_num_threads)
            data.max_num_threads = source.max_num_threads;
        /// n_wait_thds is not reported.
        data.num_waiting_threads.store(0, std::memory_order_relaxed);
        data.num_owner_switches += source.num_owner_switches;
        data.num_lock_ops += source.num_lock_ops;
    }

    /// Update `data` to the per-field maximum. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_max_update
    ALLOCATOR_ALWAYS_INLINE void profilingMaxUpdate(ThreadState * thread_state, MutexProfilingData & data)
    {
        const MutexProfilingData & source = profiling_data;
        assertOwner(thread_state);
        if (source.total_wait_time.compare(data.total_wait_time) > 0)
            data.total_wait_time.copy(source.total_wait_time);
        if (source.max_wait_time.compare(data.max_wait_time) > 0)
            data.max_wait_time.copy(source.max_wait_time);
        if (source.num_wait_times > data.num_wait_times)
            data.num_wait_times = source.num_wait_times;
        if (source.num_spin_acquired > data.num_spin_acquired)
            data.num_spin_acquired = source.num_spin_acquired;
        if (source.max_num_threads > data.max_num_threads)
            data.max_num_threads = source.max_num_threads;
        if (source.num_owner_switches > data.num_owner_switches)
            data.num_owner_switches = source.num_owner_switches;
        if (source.num_lock_ops > data.num_lock_ops)
            data.num_lock_ops = source.num_lock_ops;
        /// n_wait_thds is not reported.
    }

    /// Direct access to the profiling counters (e.g. for tests; reading requires holding the mutex).
    const MutexProfilingData & profilingData() const { return profiling_data; }

    /// The underlying pthread mutex (for `pthread_cond_wait` in the background thread: `&info->mutex.lock`).
    pthread_mutex_t * nativeHandle() { return &lock_; }

    /// With `JEMALLOC_MUTEX_INIT_CB` (FreeBSD), initializes the mutexes whose initialization was postponed.
    /// Returns true on error.
    /// jemalloc: malloc_mutex_boot
    static bool boot();

    /// The contended path: spin, then block. Leaves the mutex locked.
    /// jemalloc: malloc_mutex_lock_slow
    ALLOCATOR_NOINLINE void lockSlow();

    /// Sets the `locked` hint directly: the background thread clears it before `pthread_cond_timedwait` on
    /// `nativeHandle()` and sets it after the wait returns (`background_thread.c`), without touching the counters.
    ALLOCATOR_ALWAYS_INLINE void setLockedFlag(bool value) { locked.store(value, std::memory_order_relaxed); }

private:
    struct Empty
    {
    };

    /// The data is not touched by the mutex holder during unlocking, while it may be modified by contenders; having
    /// it before the mutex itself avoids prefetching a modified cache line for the unlocking thread.
    MutexProfilingData profiling_data;
    /// Hint flag to avoid exclusive cache line contention during spin waiting. Modified by the lock owner only
    /// (after acquired, and before release), and may be read by other threads.
    std::atomic<bool> locked{false};
    pthread_mutex_t lock_ = PTHREAD_MUTEX_INITIALIZER;
    /// With `JEMALLOC_MUTEX_INIT_CB`: the list of mutexes whose initialization is postponed until `boot`.
    [[no_unique_address, maybe_unused]] std::conditional_t<config::mutex_init_callback, Mutex *, Empty> postponed_next{};

    /// jemalloc: malloc_mutex_lock_final
    ALLOCATOR_ALWAYS_INLINE void lockFinal()
    {
        pthread_mutex_lock(&lock_);
        locked.store(true, std::memory_order_relaxed);
    }

    /// Returns true on failure (like jemalloc).
    /// jemalloc: malloc_mutex_trylock_final
    ALLOCATOR_ALWAYS_INLINE bool tryLockFinal()
    {
        bool failed = pthread_mutex_trylock(&lock_) != 0;
        if (!failed)
            locked.store(true, std::memory_order_relaxed);
        return failed;
    }

    /// jemalloc: mutex_owner_stats_update
    ALLOCATOR_ALWAYS_INLINE void ownerStatsUpdate(ThreadState * thread_state)
    {
        if constexpr (config::stats)
        {
            MutexProfilingData & data = profiling_data;
            ++data.num_lock_ops;
            if (data.prev_owner != thread_state)
            {
                data.prev_owner = thread_state;
                ++data.num_owner_switches;
            }
        }
    }
};

/// The size of `malloc_mutex_t` (release build): `mutex_prof_data_t`, `atomic_b_t` padded to the alignment of
/// `pthread_mutex_t`, `pthread_mutex_t` (and `postponed_next` with `JEMALLOC_MUTEX_INIT_CB`). The witness (56 bytes)
/// is in a union with these and does not add to the size.
static_assert(
    sizeof(Mutex)
    == alignmentCeiling(sizeof(MutexProfilingData) + 1, alignof(pthread_mutex_t)) + sizeof(pthread_mutex_t)
        + (config::mutex_init_callback ? sizeof(void *) : 0));
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Mutex) == 120, "malloc_mutex_t is 120 bytes on aarch64 glibc");
#elif defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
static_assert(sizeof(Mutex) == 112, "malloc_mutex_t is 112 bytes on x86_64 glibc");
#endif

/// RAII lock guard.
class MutexLock
{
public:
    ALLOCATOR_ALWAYS_INLINE MutexLock(ThreadState * thread_state_, Mutex & mutex_)
        : thread_state(thread_state_)
        , mutex(mutex_)
    {
        mutex.lock(thread_state);
    }

    ALLOCATOR_ALWAYS_INLINE ~MutexLock() { mutex.unlock(thread_state); }

    MutexLock(const MutexLock &) = delete;
    MutexLock & operator=(const MutexLock &) = delete;

private:
    ThreadState * thread_state;
    Mutex & mutex;
};

}
