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
#include <allocator/NsTime.h>
#include <allocator/Options.h>

#include <atomic>
#include <cstdint>
#include <pthread.h>
#include <type_traits>

namespace jemalloc
{

class ThreadState;

/// The number of CPUs, set during initialization (`malloc_ncpus`); 0 before that.
/// jemalloc: ncpus (`src/jemalloc.c`). Defined in Init.cpp.
extern unsigned ncpus;

/// With `JEMALLOC_LAZY_LOCK` (FreeBSD) mutexes are not locked until the process goes multi-threaded: the
/// `pthread_create` wrapper sets this. Elsewhere `isThreaded()` is the constant true.
/// jemalloc: isthreaded
extern bool isthreaded;

JE_ALWAYS_INLINE bool isThreaded()
{
    if constexpr (config::lazy_lock)
        return isthreaded;
    else
        return true;
}

/// FreeBSD (`JEMALLOC_MUTEX_INIT_CB`): the `calloc` used by libc to allocate the internals of `pthread_mutex_t`
/// (allocates from arena 0, `a0ialloc(num * size, zero = true, is_internal = false)`); defined by the initialization
/// code.
/// jemalloc: bootstrap_calloc
void * bootstrapCalloc(size_t num, size_t size);

/// Lock ranks (jemalloc: `witness_rank_t`, `witness.h`). A thread may only acquire a mutex with a rank strictly
/// greater than every held one (or equal for `AddressOrdered` mutexes in ascending address order).
/// Documentation only: there is no witness checking.
enum class MutexRank : unsigned
{
    /// Ignored by the witness machinery.
    OMIT,
    MIN,
    INIT = MIN,
    CTL,
    TCACHES,
    ARENAS,
    BACKGROUND_THREAD_GLOBAL,
    PROF_DUMP,
    PROF_BT2GCTX,
    PROF_TDATAS,
    PROF_TDATA,
    PROF_LOG,
    PROF_GCTX,
    PROF_RECENT_DUMP,
    BACKGROUND_THREAD,
    /// The minimally ranked core lock.
    CORE,
    DECAY = CORE,
    TCACHE_QL,
    SEC_BIN,
    EXTENT_GROW,
    HPA_SHARD_GROW = EXTENT_GROW,
    SAN_BUMP_ALLOC = EXTENT_GROW,
    EXTENTS,
    HPA_SHARD = EXTENTS,
    HPA_CENTRAL_GROW,
    HPA_CENTRAL,
    EDATA_CACHE,
    RTREE,
    BASE,
    ARENA_LARGE,
    HOOK,

    LEAF = 0x1000,
    BIN = LEAF,
    ARENA_STATS = LEAF,
    COUNTER_ACCUM = LEAF,
    DSS = LEAF,
    PROF_ACTIVE = LEAF,
    PROF_DUMP_FILENAME = LEAF,
    PROF_GDUMP = LEAF,
    PROF_NEXT_THR_UID = LEAF,
    PROF_RECENT_ALLOC = LEAF,
    PROF_STATS = LEAF,
    PROF_THREAD_ACTIVE_INIT = LEAF,
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
enum MutexProfGlobalInd : unsigned
{
    global_prof_mutex_background_thread,
    global_prof_mutex_max_per_bg_thd,
    global_prof_mutex_ctl,
    global_prof_mutex_prof,
    global_prof_mutex_prof_thds_data,
    global_prof_mutex_prof_dump,
    global_prof_mutex_prof_recent_alloc,
    global_prof_mutex_prof_recent_dump,
    global_prof_mutex_prof_stats,
    mutex_prof_num_global_mutexes,
};

inline constexpr const char * mutex_prof_global_names[mutex_prof_num_global_mutexes] = {
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
enum MutexProfArenaInd : unsigned
{
    arena_prof_mutex_large,
    arena_prof_mutex_extent_avail,
    arena_prof_mutex_extents_dirty,
    arena_prof_mutex_extents_muzzy,
    arena_prof_mutex_extents_retained,
    arena_prof_mutex_decay_dirty,
    arena_prof_mutex_decay_muzzy,
    arena_prof_mutex_base,
    arena_prof_mutex_tcache_list,
    arena_prof_mutex_hpa_shard,
    arena_prof_mutex_hpa_shard_grow,
    arena_prof_mutex_hpa_sec,
    mutex_prof_num_arena_mutexes,
};

inline constexpr const char * mutex_prof_arena_names[mutex_prof_num_arena_mutexes] = {
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
/// `derived` counters are rates (`(#/sec)`) computed from `base_counter`.
/// jemalloc: MUTEX_PROF_UINT64_COUNTERS, MUTEX_PROF_UINT32_COUNTERS
enum MutexProfUint64CounterInd : unsigned
{
    mutex_counter_num_ops,
    mutex_counter_num_ops_ps,
    mutex_counter_num_wait,
    mutex_counter_num_wait_ps,
    mutex_counter_num_spin_acq,
    mutex_counter_num_spin_acq_ps,
    mutex_counter_num_owner_switch,
    mutex_counter_num_owner_switch_ps,
    mutex_counter_total_wait_time,
    mutex_counter_total_wait_time_ps,
    mutex_counter_max_wait_time,
    mutex_prof_num_uint64_t_counters,
};

enum MutexProfUint32CounterInd : unsigned
{
    mutex_counter_max_num_thds,
    mutex_prof_num_uint32_t_counters,
};

struct MutexProfCounterInfo
{
    const char * name;
    const char * human;
    bool derived;
    unsigned base_counter;
};

inline constexpr MutexProfCounterInfo mutex_prof_uint64_counters[mutex_prof_num_uint64_t_counters] = {
    {"num_ops", "n_lock_ops", false, mutex_counter_num_ops},
    {"num_ops_ps", "(#/sec)", true, mutex_counter_num_ops},
    {"num_wait", "n_waiting", false, mutex_counter_num_wait},
    {"num_wait_ps", "(#/sec)", true, mutex_counter_num_wait},
    {"num_spin_acq", "n_spin_acq", false, mutex_counter_num_spin_acq},
    {"num_spin_acq_ps", "(#/sec)", true, mutex_counter_num_spin_acq},
    {"num_owner_switch", "n_owner_switch", false, mutex_counter_num_owner_switch},
    {"num_owner_switch_ps", "(#/sec)", true, mutex_counter_num_owner_switch},
    {"total_wait_time", "total_wait_ns", false, mutex_counter_total_wait_time},
    {"total_wait_time_ps", "(#/sec)", true, mutex_counter_total_wait_time},
    {"max_wait_time", "max_wait_ns", false, mutex_counter_max_wait_time},
};

inline constexpr MutexProfCounterInfo mutex_prof_uint32_counters[mutex_prof_num_uint32_t_counters] = {
    {"max_num_thds", "max_n_thds", false, mutex_counter_max_num_thds},
};

/// jemalloc: mutex_prof_data_t
///
/// Zero-filled memory is a valid initial state (like `LOCK_PROF_DATA_INITIALIZER`).
struct MutexProfData
{
    /// Counters touched on the slow path, i.e. when there is lock contention. Updated once we have the lock.

    /// Total time spent waiting on this mutex.
    NsTime tot_wait_time = NsTime::zero();
    /// Max time spent on a single lock operation.
    NsTime max_wait_time = NsTime::zero();
    /// # of times have to wait for this mutex (after spinning).
    uint64_t n_wait_times = 0;
    /// # of times acquired the mutex through local spinning.
    uint64_t n_spin_acquired = 0;
    /// Max # of threads waiting for the mutex at the same time.
    uint32_t max_n_thds = 0;
    /// Current # of threads waiting on the lock (modified without holding the lock).
    std::atomic<uint32_t> n_waiting_thds{0};

    /// Data touched on the fast path, right after acquiring the lock (placed right before the lock to share its
    /// cache line).

    /// # of times the mutex holder is different than the previous one.
    uint64_t n_owner_switches = 0;
    /// Previous mutex holder, to facilitate n_owner_switches.
    ThreadState * prev_owner = nullptr;
    /// # of lock() operations in total.
    uint64_t n_lock_ops = 0;

    constexpr MutexProfData() = default;
    MutexProfData(const MutexProfData &) = delete;
    MutexProfData & operator=(const MutexProfData &) = delete;

    /// jemalloc: mutex_prof_data_init
    void reset()
    {
        tot_wait_time.initZero();
        max_wait_time.initZero();
        n_wait_times = 0;
        n_spin_acquired = 0;
        max_n_thds = 0;
        n_waiting_thds.store(0, std::memory_order_relaxed);
        n_owner_switches = 0;
        prev_owner = nullptr;
        n_lock_ops = 0;
    }

    /// A member-for-member copy (including `prev_owner`), except `n_waiting_thds` which is not reported and is zeroed.
    /// jemalloc: malloc_mutex_prof_copy
    void copyFrom(const MutexProfData & source)
    {
        tot_wait_time = source.tot_wait_time;
        max_wait_time = source.max_wait_time;
        n_wait_times = source.n_wait_times;
        n_spin_acquired = source.n_spin_acquired;
        max_n_thds = source.max_n_thds;
        n_owner_switches = source.n_owner_switches;
        prev_owner = source.prev_owner;
        n_lock_ops = source.n_lock_ops;
        n_waiting_thds.store(0, std::memory_order_relaxed);
    }

    /// Aggregate (this is the sum).
    /// jemalloc: malloc_mutex_prof_merge
    void merge(const MutexProfData & data)
    {
        tot_wait_time.add(data.tot_wait_time);
        if (max_wait_time.compare(data.max_wait_time) < 0)
            max_wait_time.copy(data.max_wait_time);
        n_wait_times += data.n_wait_times;
        n_spin_acquired += data.n_spin_acquired;
        if (max_n_thds < data.max_n_thds)
            max_n_thds = data.max_n_thds;
        uint32_t cur_n_waiting_thds = n_waiting_thds.load(std::memory_order_relaxed);
        uint32_t new_n_waiting_thds = cur_n_waiting_thds + data.n_waiting_thds.load(std::memory_order_relaxed);
        n_waiting_thds.store(new_n_waiting_thds, std::memory_order_relaxed);
        n_owner_switches += data.n_owner_switches;
        n_lock_ops += data.n_lock_ops;
    }
};

static_assert(sizeof(MutexProfData) == 64, "Must have the size of mutex_prof_data_t");

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
    JE_ALWAYS_INLINE void lock(ThreadState * tsdn)
    {
        if (isThreaded())
        {
            if (trylockFinal())
                lockSlow();
            JE_ASSERT(isLocked());
            ownerStatsUpdate(tsdn);
        }
    }

    /// Returns true if the lock is acquired (note: jemalloc's `malloc_mutex_trylock` returns true on failure).
    /// jemalloc: malloc_mutex_trylock
    JE_ALWAYS_INLINE bool tryLock(ThreadState * tsdn)
    {
        if (isThreaded())
        {
            if (trylockFinal())
                return false;
            JE_ASSERT(isLocked());
            ownerStatsUpdate(tsdn);
        }
        return true;
    }

    /// jemalloc: malloc_mutex_unlock
    JE_ALWAYS_INLINE void unlock(ThreadState * /*tsdn*/)
    {
        if (isThreaded())
        {
            JE_ASSERT(isLocked());
            locked.store(false, std::memory_order_relaxed);
            pthread_mutex_unlock(&lock_);
        }
    }

    /// For sanity checking only: whether some thread holds the lock.
    /// jemalloc: malloc_mutex_is_locked
    JE_ALWAYS_INLINE bool isLocked() const { return locked.load(std::memory_order_relaxed); }

    /// Without witness, only checks that the mutex is locked (by somebody).
    /// jemalloc: malloc_mutex_assert_owner
    JE_ALWAYS_INLINE void assertOwner(ThreadState * /*tsdn*/) const
    {
        if (isThreaded())
            JE_ASSERT(isLocked());
    }

    /// A no-op without witness.
    /// jemalloc: malloc_mutex_assert_not_owner
    JE_ALWAYS_INLINE void assertNotOwner(ThreadState * /*tsdn*/) const { }

    /// jemalloc: malloc_mutex_prefork
    void prefork(ThreadState * tsdn);
    /// jemalloc: malloc_mutex_postfork_parent
    void postforkParent(ThreadState * tsdn);
    /// Re-initializes the mutex (just unlocks it with `JEMALLOC_MUTEX_INIT_CB`).
    /// jemalloc: malloc_mutex_postfork_child
    void postforkChild(ThreadState * tsdn);

    /// Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_data_reset
    void profDataReset(ThreadState * tsdn);

    /// Copy the prof data for processing. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_read
    JE_ALWAYS_INLINE void profRead(ThreadState * tsdn, MutexProfData & data)
    {
        assertOwner(tsdn);
        data.copyFrom(prof_data);
    }

    /// Accumulate the prof data into `data`. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_accum
    JE_ALWAYS_INLINE void profAccum(ThreadState * tsdn, MutexProfData & data)
    {
        const MutexProfData & source = prof_data;
        assertOwner(tsdn);
        data.tot_wait_time.add(source.tot_wait_time);
        if (source.max_wait_time.compare(data.max_wait_time) > 0)
            data.max_wait_time.copy(source.max_wait_time);
        data.n_wait_times += source.n_wait_times;
        data.n_spin_acquired += source.n_spin_acquired;
        if (data.max_n_thds < source.max_n_thds)
            data.max_n_thds = source.max_n_thds;
        /// n_wait_thds is not reported.
        data.n_waiting_thds.store(0, std::memory_order_relaxed);
        data.n_owner_switches += source.n_owner_switches;
        data.n_lock_ops += source.n_lock_ops;
    }

    /// Update `data` to the per-field maximum. Must hold the mutex.
    /// jemalloc: malloc_mutex_prof_max_update
    JE_ALWAYS_INLINE void profMaxUpdate(ThreadState * tsdn, MutexProfData & data)
    {
        const MutexProfData & source = prof_data;
        assertOwner(tsdn);
        if (source.tot_wait_time.compare(data.tot_wait_time) > 0)
            data.tot_wait_time.copy(source.tot_wait_time);
        if (source.max_wait_time.compare(data.max_wait_time) > 0)
            data.max_wait_time.copy(source.max_wait_time);
        if (source.n_wait_times > data.n_wait_times)
            data.n_wait_times = source.n_wait_times;
        if (source.n_spin_acquired > data.n_spin_acquired)
            data.n_spin_acquired = source.n_spin_acquired;
        if (source.max_n_thds > data.max_n_thds)
            data.max_n_thds = source.max_n_thds;
        if (source.n_owner_switches > data.n_owner_switches)
            data.n_owner_switches = source.n_owner_switches;
        if (source.n_lock_ops > data.n_lock_ops)
            data.n_lock_ops = source.n_lock_ops;
        /// n_wait_thds is not reported.
    }

    /// Direct access to the profiling counters (e.g. for tests; reading requires holding the mutex).
    const MutexProfData & profData() const { return prof_data; }

    /// The underlying pthread mutex (for `pthread_cond_wait` in the background thread: `&info->mtx.lock`).
    pthread_mutex_t * nativeHandle() { return &lock_; }

    /// With `JEMALLOC_MUTEX_INIT_CB` (FreeBSD), initializes the mutexes whose initialization was postponed.
    /// Returns true on error.
    /// jemalloc: malloc_mutex_boot
    static bool boot();

    /// The contended path: spin, then block. Leaves the mutex locked.
    /// jemalloc: malloc_mutex_lock_slow
    JE_NOINLINE void lockSlow();

    /// Sets the `locked` hint directly: the background thread clears it before `pthread_cond_timedwait` on
    /// `nativeHandle()` and sets it after the wait returns (`background_thread.c`), without touching the counters.
    JE_ALWAYS_INLINE void setLockedFlag(bool value) { locked.store(value, std::memory_order_relaxed); }

private:
    struct Empty
    {
    };

    /// The data is not touched by the mutex holder during unlocking, while it may be modified by contenders; having
    /// it before the mutex itself avoids prefetching a modified cache line for the unlocking thread.
    MutexProfData prof_data;
    /// Hint flag to avoid exclusive cache line contention during spin waiting. Modified by the lock owner only
    /// (after acquired, and before release), and may be read by other threads.
    std::atomic<bool> locked{false};
    pthread_mutex_t lock_ = PTHREAD_MUTEX_INITIALIZER;
    /// With `JEMALLOC_MUTEX_INIT_CB`: the list of mutexes whose initialization is postponed until `boot`.
    [[no_unique_address, maybe_unused]] std::conditional_t<config::mutex_init_cb, Mutex *, Empty> postponed_next{};

    /// jemalloc: malloc_mutex_lock_final
    JE_ALWAYS_INLINE void lockFinal()
    {
        pthread_mutex_lock(&lock_);
        locked.store(true, std::memory_order_relaxed);
    }

    /// Returns true on failure (like jemalloc).
    /// jemalloc: malloc_mutex_trylock_final
    JE_ALWAYS_INLINE bool trylockFinal()
    {
        bool failed = pthread_mutex_trylock(&lock_) != 0;
        if (!failed)
            locked.store(true, std::memory_order_relaxed);
        return failed;
    }

    /// jemalloc: mutex_owner_stats_update
    JE_ALWAYS_INLINE void ownerStatsUpdate(ThreadState * tsdn)
    {
        if constexpr (config::stats)
        {
            MutexProfData & data = prof_data;
            ++data.n_lock_ops;
            if (data.prev_owner != tsdn)
            {
                data.prev_owner = tsdn;
                ++data.n_owner_switches;
            }
        }
    }
};

/// The size of `malloc_mutex_t` (release build): `mutex_prof_data_t`, `atomic_b_t` padded to the alignment of
/// `pthread_mutex_t`, `pthread_mutex_t` (and `postponed_next` with `JEMALLOC_MUTEX_INIT_CB`). The witness (56 bytes)
/// is in a union with these and does not add to the size.
static_assert(
    sizeof(Mutex)
    == alignmentCeiling(sizeof(MutexProfData) + 1, alignof(pthread_mutex_t)) + sizeof(pthread_mutex_t)
        + (config::mutex_init_cb ? sizeof(void *) : 0));
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Mutex) == 120, "malloc_mutex_t is 120 bytes on aarch64 glibc");
#elif defined(__linux__) && defined(__GLIBC__) && defined(__x86_64__)
static_assert(sizeof(Mutex) == 112, "malloc_mutex_t is 112 bytes on x86_64 glibc");
#endif

/// RAII lock guard.
class MutexLock
{
public:
    JE_ALWAYS_INLINE MutexLock(ThreadState * tsdn_, Mutex & mutex_)
        : tsdn(tsdn_)
        , mutex(mutex_)
    {
        mutex.lock(tsdn);
    }

    JE_ALWAYS_INLINE ~MutexLock() { mutex.unlock(tsdn); }

    MutexLock(const MutexLock &) = delete;
    MutexLock & operator=(const MutexLock &) = delete;

private:
    ThreadState * tsdn;
    Mutex & mutex;
};

}
