#pragma once

/// Thread-specific data (jemalloc: `tsd_t`, `tsd.h`, `tsd_internals.h`, `tsd_types.h`, `tsd_tls.h`,
/// `tsd_malloc_thread_cleanup.h`, `tsd_generic.h`, `src/thread_state.c`).
///
/// The field order is that of jemalloc: the slow data first, then `state`, then the fast data ending with the tcache
/// (so that the fast fields share cachelines). The fields of dropped features `sec_shard` and `in_hook` are kept as
/// unused placeholders, so that the offsets of all fields are the same as in jemalloc (the alignment of the hot fields
/// relative to cachelines measurably affects the fast paths); so is `witness_tsd` (after the fast data), so that
/// `sizeof` is the same too (the address of the thread-local TSD seeds the per-thread PRNG and, with variant II TLS,
/// depends on the size of the TLS segment).
///
/// The object is constant-initialized (`constexpr` constructor = `TSD_INITIALIZER`) and trivially destructible, so it
/// can be a `thread_local`. The implementation of the per-thread storage is selected by `config::thread_state_impl`:
///   - `ThreadStateTLS` (Linux): an initial-exec `thread_local` + a pthread key whose destructor cleans up;
///   - `ThreadStateMallocThreadCleanup` (FreeBSD): `thread_local` + libc's `_malloc_thread_cleanup` hook;
///   - `ThreadStateGeneric` (Darwin): a heap-allocated wrapper found with `pthread_getspecific`.
///
/// The `thread_local` is never accessed by its address directly: `tlsAddrThreadStateTLS` re-reads the thread pointer with a
/// volatile asm and adds an offset captured out of line, so that the compiler (especially under LTO) cannot cache the
/// address across a user-space context switch (fibers migrating between threads; jemalloc issue 2890, ClickHouse fork
/// patches ddd5390f, 1a1af946). Never cache a `ThreadState *` across a point where a fiber may switch threads.
///
/// Conventions: a function that takes jemalloc's `tsd_t *` takes `ThreadState &`; `tsdn_t *` becomes a nullable
/// `ThreadState * thread_state`.

#include <allocator/Common.h>
#include <allocator/RadixTree.h>
#include <allocator/SizeClassConstants.h>
#include <allocator/ThreadCacheData.h>
#include <allocator/ThreadEventData.h>
#include <allocator/Ticker.h>

#include <atomic>
#include <cstdint>
#include <type_traits>
#include <pthread.h>

/// JEMALLOC_TLS_MODEL_INITIAL_EXEC: defined everywhere except Linux aarch64 musl (must match
/// `config::tls_model_initial_exec`; a preprocessor condition because it selects an attribute and inline asm).
#if defined(__linux__) && defined(__aarch64__) && defined(ALLOCATOR_MUSL) && ALLOCATOR_MUSL
#define ALLOCATOR_TLS_MODEL_INITIAL_EXEC 0
#else
#define ALLOCATOR_TLS_MODEL_INITIAL_EXEC 1
#endif

/// The fast fiber-safe TLS access: read the thread pointer with a volatile asm and add an offset (jemalloc:
/// `JEMALLOC_TLS_ADDR` "static-TLS fast path"). Otherwise a `noinline` accessor with a memory clobber is used.
#if defined(__GNUC__) && ALLOCATOR_TLS_MODEL_INITIAL_EXEC && (defined(__aarch64__) || defined(__x86_64__))
#define ALLOCATOR_TLS_ADDR_FAST 1
#else
#define ALLOCATOR_TLS_ADDR_FAST 0
#endif

namespace jemalloc
{

static_assert(bool(ALLOCATOR_TLS_MODEL_INITIAL_EXEC) == config::tls_model_initial_exec);

class Arena;
class ProfilingThreadData;

/// Whether the slow paths of malloc/free must be taken (some option like `junk`, `zero`, `utrace`, `abort_on_out_of_memory` is on).
/// Computed by the initialization (`malloc_slow_flag_init`); true until then. Defined in InitState.cpp.
/// jemalloc: malloc_slow
extern bool malloc_slow;

/// jemalloc: ARENA_DECAY_NTICKS_PER_UPDATE
inline constexpr int32_t ARENA_DECAY_NUM_TICKS_PER_UPDATE = 1000;

/// jemalloc: TSD_MIN_INIT_STATE_MAX_FETCHED
inline constexpr uint8_t THREAD_STATE_MIN_INIT_STATE_MAX_FETCHED = 128;

/// jemalloc: tsd_state_* (an anonymous enum of `uint8_t` values)
enum ThreadStateStatus : uint8_t
{
    /// Common case --> jnz.
    thread_state_nominal = 0,
    /// Initialized but on slow path.
    thread_state_nominal_slow = 1,
    /// Some thread has changed global state in such a way that all nominal threads need to recompute their fast /
    /// slow status the next time they get a chance. Any thread can change another thread's status *to* recompute, but
    /// threads are the only ones who can change their status *from* recompute.
    thread_state_nominal_recompute = 2,
    /// The nominal states above are lower values; this separates them from threads in the process of being born /
    /// dying.
    thread_state_nominal_max = 2,
    /// A thread might free() during its death as its only allocator action; in such scenarios, we need tsd, but set
    /// up in such a way that no cleanup is necessary.
    thread_state_minimal_initialized = 3,
    /// States during which we know we're in thread death.
    thread_state_purgatory = 4,
    thread_state_reincarnated = 5,
    /// TSD that hasn't been initialized. Even when the tsd struct lives in TLS, we need to keep track of things like
    /// whether or not our pthread destructors have been scheduled, so this is different from the nominal state.
    thread_state_uninitialized = 6,
};

/// jemalloc: tsd_binshards_t
struct ThreadStateBinShards
{
    uint8_t bin_shard[SIZE_CLASS_NUM_BINS];
};

/// jemalloc: tsd_t
class ThreadState
{
public:
    /// jemalloc: TSD_INITIALIZER
    constexpr ThreadState() = default;

    ThreadState(const ThreadState &) = delete;
    ThreadState & operator=(const ThreadState &) = delete;

    /// --- Fetching the current thread's TSD ----------------------------------------------------------------------

    /// Returns null only if `!init` and the implementation allocates (`ThreadStateGeneric`) and there is no TSD yet.
    /// jemalloc: tsd_fetch_impl
    static ALLOCATOR_ALWAYS_INLINE ThreadState * fetchImpl(bool init, bool minimal);

    /// jemalloc: tsd_fetch
    static ALLOCATOR_ALWAYS_INLINE ThreadState & fetch() { return *fetchImpl(true, false); }

    /// A minimal TSD that requires no cleanup (for threads that only free, see `free_default`).
    /// jemalloc: tsd_fetch_min
    static ALLOCATOR_ALWAYS_INLINE ThreadState & fetchMin() { return *fetchImpl(true, true); }

    /// For internal background threads use only: the reincarnated state prevents full initialization.
    /// jemalloc: tsd_internal_fetch
    static ALLOCATOR_ALWAYS_INLINE ThreadState & internalFetch()
    {
        ThreadState & thread_state = fetchMin();
        thread_state.stateSet(thread_state_reincarnated);
        return thread_state;
    }

    /// Null before the TSD is booted.
    /// jemalloc: tsdn_fetch
    static ALLOCATOR_ALWAYS_INLINE ThreadState * threadStateFetch();

    /// jemalloc: tsd_booted_get
    static ALLOCATOR_ALWAYS_INLINE bool booted();

    /// --- State --------------------------------------------------------------------------------------------------

    /// The state byte is read non-atomically in jemalloc (`tsd_state_get`); a relaxed load compiles to the same.
    /// jemalloc: tsd_state_get
    ALLOCATOR_ALWAYS_INLINE uint8_t stateGet() const { return state.load(std::memory_order_relaxed); }

    /// jemalloc: tsd_assert_fast
    ALLOCATOR_ALWAYS_INLINE void assertFast() const
    {
        /// The global slowness counter is not included: it may change asynchronously.
        ALLOCATOR_ASSERT(!malloc_slow && thread_cache_enabled && reentrancy_level == 0);
    }

    /// jemalloc: tsd_fast
    ALLOCATOR_ALWAYS_INLINE bool fast() const
    {
        bool is_fast = (stateGet() == thread_state_nominal);
        if (is_fast)
            assertFast();
        return is_fast;
    }

    /// jemalloc: tsd_nominal
    ALLOCATOR_ALWAYS_INLINE bool nominal() const
    {
        bool is_nominal = stateGet() <= thread_state_nominal_max;
        ALLOCATOR_ASSERT(is_nominal || reentrancy_level > 0);
        return is_nominal;
    }

    /// jemalloc: tsd_state_nocleanup
    ALLOCATOR_ALWAYS_INLINE bool stateNoCleanup() const
    {
        return stateGet() == thread_state_reincarnated || stateGet() == thread_state_minimal_initialized;
    }

    /// jemalloc: tsd_fetch_slow
    ThreadState & fetchSlow(bool minimal);

    /// jemalloc: tsd_state_set
    void stateSet(uint8_t new_state);

    /// Recomputes the nominal / nominal_slow state and the fast thresholds.
    /// jemalloc: tsd_slow_update
    void slowUpdate();

    /// These "raw" reentrancy functions don't check that we're not touching arena 0; prefer `preReentrancy`.
    /// jemalloc: tsd_pre_reentrancy_raw
    ALLOCATOR_ALWAYS_INLINE void preReentrancyRaw()
    {
        bool was_fast = fast();
        ALLOCATOR_ASSERT(reentrancy_level < INT8_MAX);
        ++reentrancy_level;
        if (was_fast)
        {
            /// Prepare the slow path for reentrancy.
            slowUpdate();
            ALLOCATOR_ASSERT(stateGet() == thread_state_nominal_slow);
        }
    }

    /// jemalloc: tsd_post_reentrancy_raw
    ALLOCATOR_ALWAYS_INLINE void postReentrancyRaw()
    {
        ALLOCATOR_ASSERT(reentrancy_level > 0);
        if (--reentrancy_level == 0)
            slowUpdate();
    }

    /// Call `globalSlowIncrement` when a module wants to take all threads down the slow paths, and `globalSlowDecrement` when it
    /// no longer needs to.
    /// jemalloc: tsd_global_slow_inc, tsd_global_slow_dec, tsd_global_slow
    static void globalSlowIncrement(ThreadState * thread_state);
    static void globalSlowDecrement(ThreadState * thread_state);
    static bool globalSlow();

    /// --- Boot, thread exit, fork --------------------------------------------------------------------------------

    /// Initializes the nominal list lock and the TSD implementation, then fetches (fully initializes) the TSD of the
    /// initializing thread. Returns null on error.
    /// jemalloc: malloc_tsd_boot0
    static ThreadState * mallocThreadStateBoot0();

    /// jemalloc: malloc_tsd_boot1
    static void mallocThreadStateBoot1();

    /// The thread exit destructor.
    /// jemalloc: tsd_cleanup
    static void cleanup(void * arg);

    /// jemalloc: tsd_prefork, tsd_postfork_parent, tsd_postfork_child
    void prefork();
    void postforkParent();
    void postforkChild();

    /// --- Accessors used by the lower layers --------------------------------------------------------------------------

    /// jemalloc: tsd_rtree_ctx, tsd_rtree_ctxp_get, tsd_rtree_ctxp_get_unsafe
    ALLOCATOR_ALWAYS_INLINE RadixTreeContext * radixTreeContext() { return &radix_tree_context; }

    /// If the tsd cannot be accessed (null `thread_state`), initializes the fallback context and returns a pointer to it.
    /// jemalloc: tsdn_rtree_ctx
    static ALLOCATOR_ALWAYS_INLINE RadixTreeContext * threadStateRadixTreeContext(ThreadState * thread_state, RadixTreeContext * fallback)
    {
        if (ALLOCATOR_UNLIKELY(thread_state == nullptr))
        {
            fallback->init();
            return fallback;
        }
        return thread_state->radixTreeContext();
    }

    /// jemalloc: tsd_prng_statep_get
    ALLOCATOR_ALWAYS_INLINE uint64_t & prngState() { return prng_state; }

    /// jemalloc: tsd_reentrancy_level_get, tsd_reentrancy_levelp_get
    ALLOCATOR_ALWAYS_INLINE int8_t & reentrancyLevel() { return reentrancy_level; }

    /// jemalloc: tsd_tcachep_get, tsd_tcache_slowp_get
    ALLOCATOR_ALWAYS_INLINE ThreadCache * threadCacheGet() { return &thread_cache; }
    ALLOCATOR_ALWAYS_INLINE ThreadCacheSlow * threadCacheSlowGet() { return &thread_cache_slow; }

    /// --- TSD_DATA_SLOW ----------------------------------------------------------------------------------------------

    bool thread_cache_enabled = false;
    int8_t reentrancy_level = 0;
    uint8_t min_init_state_num_fetched = 0;
    uint64_t thread_allocated_last_event = 0;
    uint64_t thread_allocated_next_event = 0;
    uint64_t thread_deallocated_last_event = 0;
    uint64_t thread_deallocated_next_event = 0;
    ThreadEventData thread_event_data;
    uint64_t profiling_sample_last_event = 0;
    uint64_t stats_interval_last_event = 0;
    /// jemalloc: prof_tdata_t * prof_tdata
    ProfilingThreadData * profiling_thread_data = nullptr;
    uint64_t prng_state = 0;
    uint64_t sanitizer_extents_until_guard_small = 0;
    uint64_t sanitizer_extents_until_guard_large = 0;
    Arena * internal_arena = nullptr;
    Arena * arena = nullptr;
    TickerGeometric arena_decay_ticker = tickerGeometricInit(ARENA_DECAY_NUM_TICKS_PER_UPDATE);
    /// jemalloc: uint8_t sec_shard (unused: placeholder for the identical layout).
    uint8_t unused_small_extent_cache_shard = 0;
    /// jemalloc: TSD_BINSHARDS_ZERO_INITIALIZER = {{UINT8_MAX}}: only the first element is 255, the rest are 0.
    ThreadStateBinShards bin_shards = {{UINT8_MAX}};
    /// The link in the list of nominal TSDs. jemalloc: tsd_link_t tsd_link
    RingLink<ThreadState> thread_state_link{};
    /// jemalloc: bool in_hook (unused: placeholder for the identical layout).
    bool unused_in_hook = false;
    Peak peak;
    ActivityCallbackThunk activity_callback_thunk;
    ThreadCacheSlow thread_cache_slow;
    RadixTreeContext radix_tree_context;

    /// jemalloc: atomic_u8_t state = ATOMIC_INIT(tsd_state_uninitialized)
    std::atomic<uint8_t> state{thread_state_uninitialized};

    /// --- TSD_DATA_FAST ----------------------------------------------------------------------------------------------

    /// Exposed through `thread.allocatedp` / `thread.deallocatedp`: their addresses must be stable.
    uint64_t thread_allocated = 0;
    /// The fast thresholds are also zeroed by other threads (`threadStateForceRecompute`), so all accesses go through
    /// relaxed `std::atomic_ref` (plain loads and stores on all supported platforms).
    uint64_t thread_allocated_next_event_fast = 0;
    uint64_t thread_deallocated = 0;
    uint64_t thread_deallocated_next_event_fast = 0;
    /// The last element of the fast data.
    ThreadCache thread_cache;

    /// --- TSD_DATA_SLOWER -------------------------------------------------------------------------------------------

    /// jemalloc: witness_tsd_t witness_tsd = {witness_list_t witnesses, bool forking} (unused: there is no witness;
    /// placeholder for the identical layout).
    struct
    {
        void * witnesses = nullptr;
        bool forking = false;
    } unused_witness_thread_state;

private:
    /// jemalloc: tsd_local_slow
    bool localSlow() const { return !thread_cache_enabled || reentrancy_level > 0; }

    /// jemalloc: tsd_state_compute
    uint8_t stateCompute() const;

    /// jemalloc: tsd_prng_state_init
    void prngStateInit();

    /// jemalloc: tsd_san_init (`san.c`)
    void sanitizerInit();

    /// jemalloc: tsd_data_init
    bool dataInit();

    /// jemalloc: tsd_data_init_nocleanup
    bool dataInitNoCleanup();

    /// jemalloc: tsd_do_data_cleanup
    void doDataCleanup();

    /// jemalloc: assert_tsd_data_cleanup_done
    void assertDataCleanupDone() const;
};

static_assert(std::is_trivially_destructible_v<ThreadState>);

/// --- Fiber-safe TLS access (jemalloc: JEMALLOC_TLS_ADDR) ------------------------------------------------------------

namespace thread_state_detail
{

#if ALLOCATOR_TLS_ADDR_FAST

/// A volatile asm: not hoistable / CSE-able across calls.
/// jemalloc: jemalloc_thread_pointer
ALLOCATOR_ALWAYS_INLINE char * threadPointer()
{
    char * thread_pointer;
#if defined(__aarch64__) && defined(__APPLE__)
    __asm__ __volatile__("mrs %0, tpidrro_el0\n\tbic %0, %0, #7" : "=r"(thread_pointer));
#elif defined(__aarch64__)
    __asm__ __volatile__("mrs %0, tpidr_el0" : "=r"(thread_pointer));
#elif defined(__x86_64__) && defined(__APPLE__)
    __asm__ __volatile__("movq %%gs:0, %0" : "=r"(thread_pointer));
#else
    __asm__ __volatile__("movq %%fs:0, %0" : "=r"(thread_pointer));
#endif
    return thread_pointer;
}

/// 1 is unreachable: the TLS variable and the thread pointer are at least 4-aligned.
/// jemalloc: JEMALLOC_TLS_OFFSET_UNINITIALIZED
inline constexpr intptr_t TLS_OFFSET_UNINITIALIZED = 1;

/// The offset of the variable from the thread pointer; the same for every thread. Read and written with relaxed
/// atomics: threads racing their first allocation initialize it concurrently with the same value.
/// jemalloc: jemalloc_tls_offset_tsd_tls, jemalloc_tls_offset_tsd_initialized
extern std::atomic<intptr_t> tls_offset_thread_state_tls;
extern std::atomic<intptr_t> tls_offset_thread_state_initialized;

/// Must be out of line: computed inline as `&variable - thread_pointer`, the two terms hoist independently and cancel the
/// volatile read back to the stale address.
/// jemalloc: jemalloc_tls_offset_init_tsd_tls, jemalloc_tls_offset_init_tsd_initialized
ALLOCATOR_NOINLINE intptr_t tlsOffsetInitThreadStateTLS();
ALLOCATOR_NOINLINE intptr_t tlsOffsetInitThreadStateInitialized();

/// jemalloc: jemalloc_tls_addr_tsd_tls (JEMALLOC_TLS_ADDR(tsd_tls))
ALLOCATOR_ALWAYS_INLINE ThreadState * tlsAddrThreadStateTLS()
{
    intptr_t tls_offset = tls_offset_thread_state_tls.load(std::memory_order_relaxed);
    if (ALLOCATOR_UNLIKELY(tls_offset == TLS_OFFSET_UNINITIALIZED))
        tls_offset = tlsOffsetInitThreadStateTLS();
    return reinterpret_cast<ThreadState *>(threadPointer() + tls_offset);
}

/// jemalloc: jemalloc_tls_addr_tsd_initialized (JEMALLOC_TLS_ADDR(tsd_initialized))
ALLOCATOR_ALWAYS_INLINE bool * tlsAddrThreadStateInitialized()
{
    intptr_t tls_offset = tls_offset_thread_state_initialized.load(std::memory_order_relaxed);
    if (ALLOCATOR_UNLIKELY(tls_offset == TLS_OFFSET_UNINITIALIZED))
        tls_offset = tlsOffsetInitThreadStateInitialized();
    return reinterpret_cast<bool *>(threadPointer() + tls_offset);
}

#else

/// A `noinline` accessor returning the address laundered through an empty asm with a memory clobber: one real call
/// per access. jemalloc: JEMALLOC_TLS_ADDR (other GNU targets)
ALLOCATOR_NOINLINE ThreadState * tlsAddrThreadStateTLS();
ALLOCATOR_NOINLINE bool * tlsAddrThreadStateInitialized();

#endif

}

/// --- TSD implementations ---------------------------------------------------------------------------------------

/// Linux: a `thread_local` + a pthread key whose destructor (`ThreadState::cleanup`) runs at thread exit.
/// jemalloc: tsd_tls.h
struct ThreadStateTLS
{
    /// jemalloc: tsd_tsd
    static pthread_key_t key;
    /// jemalloc: tsd_booted
    static bool is_booted;

    /// jemalloc: tsd_get_allocates
    static constexpr bool get_allocates = false;

    /// jemalloc: tsd_boot0. Returns true on error.
    static bool boot0();
    /// jemalloc: tsd_boot1
    static void boot1() { }

    /// jemalloc: tsd_get
    static ALLOCATOR_ALWAYS_INLINE ThreadState * get(bool /*init*/) { return thread_state_detail::tlsAddrThreadStateTLS(); }
    /// Arms the destructor. jemalloc: tsd_set
    static void set(ThreadState * value);
};

/// FreeBSD: `thread_local` + `thread_state_initialized`; libthr calls `_malloc_thread_cleanup` at thread exit.
/// jemalloc: tsd_malloc_thread_cleanup.h
struct ThreadStateMallocThreadCleanup
{
    static bool is_booted;
    static constexpr bool get_allocates = false;

    /// jemalloc: tsd_cleanup_wrapper. Returns true if it must be called again.
    static bool cleanupWrapper();

    static bool boot0();
    static void boot1() { }

    static ALLOCATOR_ALWAYS_INLINE ThreadState * get(bool /*init*/) { return thread_state_detail::tlsAddrThreadStateTLS(); }
    static void set(ThreadState * value);
};

/// The maximum number of cleanup functions registered with `mallocThreadStateCleanupRegister`.
/// jemalloc: MALLOC_TSD_CLEANUPS_MAX
inline constexpr unsigned MALLOC_THREAD_STATE_CLEANUPS_MAX = 4;

/// jemalloc: malloc_tsd_cleanup_t
using MallocThreadStateCleanup = bool (*)();

/// jemalloc: _malloc_tsd_cleanup_register
void mallocThreadStateCleanupRegister(MallocThreadStateCleanup f);

/// Runs the registered cleanups until none of them asks to be run again. Exported as `_malloc_thread_cleanup` on
/// FreeBSD.
/// jemalloc: _malloc_thread_cleanup
void mallocThreadCleanup();

/// Darwin: the TSD lives in a heap-allocated wrapper found with `pthread_getspecific`.
/// jemalloc: tsd_generic.h
struct ThreadStateGeneric
{
    /// jemalloc: tsd_wrapper_t
    struct Wrapper
    {
        bool initialized = false;
        ThreadState value;
    };

    /// For the detection of recursive TSD initialization (allocation from inside the wrapper allocation).
    /// jemalloc: tsd_init_block_t
    struct InitBlock
    {
        RingLink<InitBlock> link{};
        pthread_t thread{};
        void * data = nullptr;
    };

    static pthread_key_t key;
    static bool is_booted;
    /// Used before and while booting. jemalloc: tsd_boot_wrapper
    static Wrapper boot_wrapper;

    static constexpr bool get_allocates = true;

    /// jemalloc: tsd_init_check_recursion
    static void * initCheckRecursion(InitBlock * block);
    /// jemalloc: tsd_init_finish
    static void initFinish(InitBlock * block);

    /// jemalloc: tsd_cleanup_wrapper
    static void cleanupWrapper(void * arg);
    /// jemalloc: tsd_wrapper_set
    static void wrapperSet(Wrapper * wrapper);
    /// The allocating part of `tsd_wrapper_get`.
    static ALLOCATOR_NOINLINE Wrapper * wrapperGetSlow();

    /// jemalloc: tsd_wrapper_get
    static ALLOCATOR_ALWAYS_INLINE Wrapper * wrapperGet(bool init)
    {
        if (ALLOCATOR_UNLIKELY(!is_booted))
            return &boot_wrapper;

        Wrapper * wrapper = static_cast<Wrapper *>(pthread_getspecific(key));
        if (init && ALLOCATOR_UNLIKELY(wrapper == nullptr))
            wrapper = wrapperGetSlow();
        return wrapper;
    }

    static bool boot0();
    static void boot1();

    static ALLOCATOR_ALWAYS_INLINE ThreadState * get(bool init)
    {
        ALLOCATOR_ASSERT(is_booted);
        Wrapper * wrapper = wrapperGet(init);
        if (!init && wrapper == nullptr)
            return nullptr;
        return &wrapper->value;
    }

    static void set(ThreadState * value);
};

/// The TSD implementation of this platform.
using ThreadStateStorage = std::conditional_t<
    config::thread_state_impl == ThreadStateImpl::TLS,
    ThreadStateTLS,
    std::conditional_t<
        config::thread_state_impl == ThreadStateImpl::MallocThreadCleanup,
        ThreadStateMallocThreadCleanup,
        ThreadStateGeneric>>;

ALLOCATOR_ALWAYS_INLINE bool ThreadState::booted()
{
    return ThreadStateStorage::is_booted;
}

ALLOCATOR_ALWAYS_INLINE ThreadState * ThreadState::fetchImpl(bool init, bool minimal)
{
    ThreadState * thread_state = ThreadStateStorage::get(init);

    if (!init && ThreadStateStorage::get_allocates && thread_state == nullptr)
        return nullptr;
    ALLOCATOR_ASSERT(thread_state != nullptr);

    if (ALLOCATOR_UNLIKELY(thread_state->stateGet() != thread_state_nominal))
        return &thread_state->fetchSlow(minimal);
    ALLOCATOR_ASSERT(thread_state->fast());
    thread_state->assertFast();

    return thread_state;
}

ALLOCATOR_ALWAYS_INLINE ThreadState * ThreadState::threadStateFetch()
{
    if (!booted())
        return nullptr;
    return fetchImpl(false, false);
}

/// `arena` is the current context; reentry from arena 0 is not allowed (jemalloc asserts it in debug builds only;
/// the assertion needs the arena table and is not ported).
/// jemalloc: pre_reentrancy
ALLOCATOR_ALWAYS_INLINE void preReentrancy(ThreadState & thread_state, Arena * /*arena*/)
{
    thread_state.preReentrancyRaw();
}

/// jemalloc: post_reentrancy
ALLOCATOR_ALWAYS_INLINE void postReentrancy(ThreadState & thread_state)
{
    thread_state.postReentrancyRaw();
}

/// --- Hooks of other modules, called by the TSD life cycle --------------------------------------------------------
/// Defined by the owning modules: ThreadCache.cpp, Arenas.cpp, Profiling.cpp.

/// Sets `thread_cache_enabled` from `opt.tcache` and initializes the automatic tcache. Returns true on error.
/// jemalloc: tsd_tcache_enabled_data_init (`tcache.c`)
bool threadCacheThreadStateDataInit(ThreadState & thread_state);
/// jemalloc: tcache_cleanup (`tcache.c`)
void threadCacheCleanup(ThreadState & thread_state);
/// jemalloc: arena_cleanup (`jemalloc.c`; defined in Arenas.cpp)
void arenaCleanup(ThreadState & thread_state);
/// jemalloc: iarena_cleanup (`jemalloc.c`; defined in Arenas.cpp)
void internalArenaCleanup(ThreadState & thread_state);
/// jemalloc: prof_tdata_cleanup (`prof_data.c`)
void profilingThreadDataCleanup(ThreadState & thread_state);
/// Internal allocation from arena 0 (used by `ThreadStateGeneric` for the wrappers).
/// jemalloc: a0malloc, a0dalloc (`jemalloc.c`; defined in Arenas.cpp)
void * arena0Allocate(size_t size);
void arena0Deallocate(void * ptr);

}
