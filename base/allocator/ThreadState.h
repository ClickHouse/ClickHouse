#pragma once

/// Thread-specific data (jemalloc: `tsd_t`, `tsd.h`, `tsd_internals.h`, `tsd_types.h`, `tsd_tls.h`,
/// `tsd_malloc_thread_cleanup.h`, `tsd_generic.h`, `src/tsd.c`).
///
/// The field order is that of jemalloc: the slow data first, then `state`, then the fast data ending with the tcache
/// (so that the fast fields share cachelines). The fields of dropped features `sec_shard` and `in_hook` are kept as
/// unused placeholders, so that the offsets of all fields are the same as in jemalloc (the alignment of the hot fields
/// relative to cachelines measurably affects the fast paths); so is `witness_tsd` (after the fast data), so that
/// `sizeof` is the same too (the address of the thread-local TSD seeds the per-thread PRNG and, with variant II TLS,
/// depends on the size of the TLS segment).
///
/// The object is constant-initialized (`constexpr` constructor = `TSD_INITIALIZER`) and trivially destructible, so it
/// can be a `thread_local`. The implementation of the per-thread storage is selected by `config::tsd_impl`:
///   - `TsdTls` (Linux): an initial-exec `thread_local` + a pthread key whose destructor cleans up;
///   - `TsdMallocThreadCleanup` (FreeBSD): `thread_local` + libc's `_malloc_thread_cleanup` hook;
///   - `TsdGeneric` (Darwin): a heap-allocated wrapper found with `pthread_getspecific`.
///
/// The `thread_local` is never accessed by its address directly: `tlsAddrTsdTls` re-reads the thread pointer with a
/// volatile asm and adds an offset captured out of line, so that the compiler (especially under LTO) cannot cache the
/// address across a user-space context switch (fibers migrating between threads; jemalloc issue 2890, ClickHouse fork
/// patches ddd5390f, 1a1af946). Never cache a `ThreadState *` across a point where a fiber may switch threads.
///
/// Conventions: a function that takes jemalloc's `tsd_t *` takes `ThreadState &`; `tsdn_t *` becomes a nullable
/// `ThreadState * tsdn`.

#include <allocator/Common.h>
#include <allocator/RadixTree.h>
#include <allocator/SizeClassConstants.h>
#include <allocator/ThreadCacheData.h>
#include <allocator/ThreadEventData.h>
#include <allocator/Ticker.h>

#include <atomic>
#include <cstdint>
#include <pthread.h>
#include <type_traits>

/// JEMALLOC_TLS_MODEL_INITIAL_EXEC: defined everywhere except Linux aarch64 musl (must match
/// `config::tls_model_initial_exec`; a preprocessor condition because it selects an attribute and inline asm).
#if defined(__linux__) && defined(__aarch64__) && defined(ALLOCATOR_MUSL) && ALLOCATOR_MUSL
#    define ALLOCATOR_TLS_MODEL_INITIAL_EXEC 0
#else
#    define ALLOCATOR_TLS_MODEL_INITIAL_EXEC 1
#endif

/// The fast fiber-safe TLS access: read the thread pointer with a volatile asm and add an offset (jemalloc:
/// `JEMALLOC_TLS_ADDR` "static-TLS fast path"). Otherwise a `noinline` accessor with a memory clobber is used.
#if defined(__GNUC__) && ALLOCATOR_TLS_MODEL_INITIAL_EXEC && (defined(__aarch64__) || defined(__x86_64__))
#    define ALLOCATOR_TLS_ADDR_FAST 1
#else
#    define ALLOCATOR_TLS_ADDR_FAST 0
#endif

namespace jemalloc
{

static_assert(bool(ALLOCATOR_TLS_MODEL_INITIAL_EXEC) == config::tls_model_initial_exec);

class Arena;
class ProfThreadData;

/// Whether the slow paths of malloc/free must be taken (some option like `junk`, `zero`, `utrace`, `xmalloc` is on).
/// Computed by the initialization (`malloc_slow_flag_init`); true until then. Defined in InitState.cpp.
/// jemalloc: malloc_slow
extern bool malloc_slow;

/// jemalloc: ARENA_DECAY_NTICKS_PER_UPDATE
inline constexpr int32_t ARENA_DECAY_NTICKS_PER_UPDATE = 1000;

/// jemalloc: TSD_MIN_INIT_STATE_MAX_FETCHED
inline constexpr uint8_t TSD_MIN_INIT_STATE_MAX_FETCHED = 128;

/// jemalloc: tsd_state_* (an anonymous enum of `uint8_t` values)
enum TsdState : uint8_t
{
    /// Common case --> jnz.
    tsd_state_nominal = 0,
    /// Initialized but on slow path.
    tsd_state_nominal_slow = 1,
    /// Some thread has changed global state in such a way that all nominal threads need to recompute their fast /
    /// slow status the next time they get a chance. Any thread can change another thread's status *to* recompute, but
    /// threads are the only ones who can change their status *from* recompute.
    tsd_state_nominal_recompute = 2,
    /// The nominal states above are lower values; this separates them from threads in the process of being born /
    /// dying.
    tsd_state_nominal_max = 2,
    /// A thread might free() during its death as its only allocator action; in such scenarios, we need tsd, but set
    /// up in such a way that no cleanup is necessary.
    tsd_state_minimal_initialized = 3,
    /// States during which we know we're in thread death.
    tsd_state_purgatory = 4,
    tsd_state_reincarnated = 5,
    /// Tsd that hasn't been initialized. Even when the tsd struct lives in TLS, we need to keep track of things like
    /// whether or not our pthread destructors have been scheduled, so this is different from the nominal state.
    tsd_state_uninitialized = 6,
};

/// jemalloc: tsd_binshards_t
struct TsdBinshards
{
    uint8_t binshard[SC_NBINS];
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

    /// Returns null only if `!init` and the implementation allocates (`TsdGeneric`) and there is no TSD yet.
    /// jemalloc: tsd_fetch_impl
    static JE_ALWAYS_INLINE ThreadState * fetchImpl(bool init, bool minimal);

    /// jemalloc: tsd_fetch
    static JE_ALWAYS_INLINE ThreadState & fetch() { return *fetchImpl(true, false); }

    /// A minimal TSD that requires no cleanup (for threads that only free, see `free_default`).
    /// jemalloc: tsd_fetch_min
    static JE_ALWAYS_INLINE ThreadState & fetchMin() { return *fetchImpl(true, true); }

    /// For internal background threads use only: the reincarnated state prevents full initialization.
    /// jemalloc: tsd_internal_fetch
    static JE_ALWAYS_INLINE ThreadState & internalFetch()
    {
        ThreadState & tsd = fetchMin();
        tsd.stateSet(tsd_state_reincarnated);
        return tsd;
    }

    /// Null before the TSD is booted.
    /// jemalloc: tsdn_fetch
    static JE_ALWAYS_INLINE ThreadState * tsdnFetch();

    /// jemalloc: tsd_booted_get
    static JE_ALWAYS_INLINE bool booted();

    /// --- State --------------------------------------------------------------------------------------------------

    /// The state byte is read non-atomically in jemalloc (`tsd_state_get`); a relaxed load compiles to the same.
    /// jemalloc: tsd_state_get
    JE_ALWAYS_INLINE uint8_t stateGet() const { return state.load(std::memory_order_relaxed); }

    /// jemalloc: tsd_assert_fast
    JE_ALWAYS_INLINE void assertFast() const
    {
        /// The global slowness counter is not included: it may change asynchronously.
        JE_ASSERT(!malloc_slow && tcache_enabled && reentrancy_level == 0);
    }

    /// jemalloc: tsd_fast
    JE_ALWAYS_INLINE bool fast() const
    {
        bool is_fast = (stateGet() == tsd_state_nominal);
        if (is_fast)
            assertFast();
        return is_fast;
    }

    /// jemalloc: tsd_nominal
    JE_ALWAYS_INLINE bool nominal() const
    {
        bool is_nominal = stateGet() <= tsd_state_nominal_max;
        JE_ASSERT(is_nominal || reentrancy_level > 0);
        return is_nominal;
    }

    /// jemalloc: tsd_state_nocleanup
    JE_ALWAYS_INLINE bool stateNocleanup() const
    {
        return stateGet() == tsd_state_reincarnated || stateGet() == tsd_state_minimal_initialized;
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
    JE_ALWAYS_INLINE void preReentrancyRaw()
    {
        bool was_fast = fast();
        JE_ASSERT(reentrancy_level < INT8_MAX);
        ++reentrancy_level;
        if (was_fast)
        {
            /// Prepare the slow path for reentrancy.
            slowUpdate();
            JE_ASSERT(stateGet() == tsd_state_nominal_slow);
        }
    }

    /// jemalloc: tsd_post_reentrancy_raw
    JE_ALWAYS_INLINE void postReentrancyRaw()
    {
        JE_ASSERT(reentrancy_level > 0);
        if (--reentrancy_level == 0)
            slowUpdate();
    }

    /// Call `globalSlowInc` when a module wants to take all threads down the slow paths, and `globalSlowDec` when it
    /// no longer needs to.
    /// jemalloc: tsd_global_slow_inc, tsd_global_slow_dec, tsd_global_slow
    static void globalSlowInc(ThreadState * tsdn);
    static void globalSlowDec(ThreadState * tsdn);
    static bool globalSlow();

    /// --- Boot, thread exit, fork --------------------------------------------------------------------------------

    /// Initializes the nominal list lock and the TSD implementation, then fetches (fully initializes) the TSD of the
    /// initializing thread. Returns null on error.
    /// jemalloc: malloc_tsd_boot0
    static ThreadState * mallocTsdBoot0();

    /// jemalloc: malloc_tsd_boot1
    static void mallocTsdBoot1();

    /// The thread exit destructor.
    /// jemalloc: tsd_cleanup
    static void cleanup(void * arg);

    /// jemalloc: tsd_prefork, tsd_postfork_parent, tsd_postfork_child
    void prefork();
    void postforkParent();
    void postforkChild();

    /// --- Accessors used by the lower layers --------------------------------------------------------------------------

    /// jemalloc: tsd_rtree_ctx, tsd_rtree_ctxp_get, tsd_rtree_ctxp_get_unsafe
    JE_ALWAYS_INLINE RadixTreeContext * rtreeCtx() { return &rtree_ctx; }

    /// If the tsd cannot be accessed (null `tsdn`), initializes the fallback context and returns a pointer to it.
    /// jemalloc: tsdn_rtree_ctx
    static JE_ALWAYS_INLINE RadixTreeContext * tsdnRtreeCtx(ThreadState * tsdn, RadixTreeContext * fallback)
    {
        if (JE_UNLIKELY(tsdn == nullptr))
        {
            fallback->init();
            return fallback;
        }
        return tsdn->rtreeCtx();
    }

    /// jemalloc: tsd_prng_statep_get
    JE_ALWAYS_INLINE uint64_t & prngState() { return prng_state; }

    /// jemalloc: tsd_reentrancy_level_get, tsd_reentrancy_levelp_get
    JE_ALWAYS_INLINE int8_t & reentrancyLevel() { return reentrancy_level; }

    /// jemalloc: tsd_tcachep_get, tsd_tcache_slowp_get
    JE_ALWAYS_INLINE ThreadCache * tcacheGet() { return &tcache; }
    JE_ALWAYS_INLINE ThreadCacheSlow * tcacheSlowGet() { return &tcache_slow; }

    /// --- TSD_DATA_SLOW ----------------------------------------------------------------------------------------------

    bool tcache_enabled = false;
    int8_t reentrancy_level = 0;
    uint8_t min_init_state_nfetched = 0;
    uint64_t thread_allocated_last_event = 0;
    uint64_t thread_allocated_next_event = 0;
    uint64_t thread_deallocated_last_event = 0;
    uint64_t thread_deallocated_next_event = 0;
    ThreadEventData te_data;
    uint64_t prof_sample_last_event = 0;
    uint64_t stats_interval_last_event = 0;
    /// jemalloc: prof_tdata_t * prof_tdata
    ProfThreadData * prof_tdata = nullptr;
    uint64_t prng_state = 0;
    uint64_t san_extents_until_guard_small = 0;
    uint64_t san_extents_until_guard_large = 0;
    Arena * iarena = nullptr;
    Arena * arena = nullptr;
    TickerGeom arena_decay_ticker = tickerGeomInit(ARENA_DECAY_NTICKS_PER_UPDATE);
    /// jemalloc: uint8_t sec_shard (unused: placeholder for the identical layout).
    uint8_t unused_sec_shard = 0;
    /// jemalloc: TSD_BINSHARDS_ZERO_INITIALIZER = {{UINT8_MAX}}: only the first element is 255, the rest are 0.
    TsdBinshards binshards = {{UINT8_MAX}};
    /// The link in the list of nominal TSDs. jemalloc: tsd_link_t tsd_link
    RingLink<ThreadState> tsd_link{};
    /// jemalloc: bool in_hook (unused: placeholder for the identical layout).
    bool unused_in_hook = false;
    Peak peak;
    ActivityCallbackThunk activity_callback_thunk;
    ThreadCacheSlow tcache_slow;
    RadixTreeContext rtree_ctx;

    /// jemalloc: atomic_u8_t state = ATOMIC_INIT(tsd_state_uninitialized)
    std::atomic<uint8_t> state{tsd_state_uninitialized};

    /// --- TSD_DATA_FAST ----------------------------------------------------------------------------------------------

    /// Exposed through `thread.allocatedp` / `thread.deallocatedp`: their addresses must be stable.
    uint64_t thread_allocated = 0;
    uint64_t thread_allocated_next_event_fast = 0;
    uint64_t thread_deallocated = 0;
    uint64_t thread_deallocated_next_event_fast = 0;
    /// The last element of the fast data.
    ThreadCache tcache;

    /// --- TSD_DATA_SLOWER -------------------------------------------------------------------------------------------

    /// jemalloc: witness_tsd_t witness_tsd = {witness_list_t witnesses, bool forking} (unused: there is no witness;
    /// placeholder for the identical layout).
    struct
    {
        void * witnesses = nullptr;
        bool forking = false;
    } unused_witness_tsd;

private:
    /// jemalloc: tsd_local_slow
    bool localSlow() const { return !tcache_enabled || reentrancy_level > 0; }

    /// jemalloc: tsd_state_compute
    uint8_t stateCompute() const;

    /// jemalloc: tsd_prng_state_init
    void prngStateInit();

    /// jemalloc: tsd_san_init (`san.c`)
    void sanInit();

    /// jemalloc: tsd_data_init
    bool dataInit();

    /// jemalloc: tsd_data_init_nocleanup
    bool dataInitNocleanup();

    /// jemalloc: tsd_do_data_cleanup
    void doDataCleanup();

    /// jemalloc: assert_tsd_data_cleanup_done
    void assertDataCleanupDone() const;
};

static_assert(std::is_trivially_destructible_v<ThreadState>);

/// --- Fiber-safe TLS access (jemalloc: JEMALLOC_TLS_ADDR) ------------------------------------------------------------

namespace tsd_detail
{

#if ALLOCATOR_TLS_ADDR_FAST

/// A volatile asm: not hoistable / CSE-able across calls.
/// jemalloc: jemalloc_thread_pointer
JE_ALWAYS_INLINE char * threadPointer()
{
    char * thread_pointer;
#    if defined(__aarch64__) && defined(__APPLE__)
    __asm__ __volatile__("mrs %0, tpidrro_el0\n\tbic %0, %0, #7" : "=r"(thread_pointer));
#    elif defined(__aarch64__)
    __asm__ __volatile__("mrs %0, tpidr_el0" : "=r"(thread_pointer));
#    elif defined(__x86_64__) && defined(__APPLE__)
    __asm__ __volatile__("movq %%gs:0, %0" : "=r"(thread_pointer));
#    else
    __asm__ __volatile__("movq %%fs:0, %0" : "=r"(thread_pointer));
#    endif
    return thread_pointer;
}

/// 1 is unreachable: the TLS variable and the thread pointer are at least 4-aligned.
/// jemalloc: JEMALLOC_TLS_OFFSET_UNINITIALIZED
inline constexpr intptr_t TLS_OFFSET_UNINITIALIZED = 1;

/// The offset of the variable from the thread pointer; the same for every thread. Read and written with relaxed
/// atomics: threads racing their first allocation initialize it concurrently with the same value.
/// jemalloc: jemalloc_tls_offset_tsd_tls, jemalloc_tls_offset_tsd_initialized
extern std::atomic<intptr_t> tls_offset_tsd_tls;
extern std::atomic<intptr_t> tls_offset_tsd_initialized;

/// Must be out of line: computed inline as `&var - thread_pointer`, the two terms hoist independently and cancel the
/// volatile read back to the stale address.
/// jemalloc: jemalloc_tls_offset_init_tsd_tls, jemalloc_tls_offset_init_tsd_initialized
JE_NOINLINE intptr_t tlsOffsetInitTsdTls();
JE_NOINLINE intptr_t tlsOffsetInitTsdInitialized();

/// jemalloc: jemalloc_tls_addr_tsd_tls (JEMALLOC_TLS_ADDR(tsd_tls))
JE_ALWAYS_INLINE ThreadState * tlsAddrTsdTls()
{
    intptr_t tls_offset = tls_offset_tsd_tls.load(std::memory_order_relaxed);
    if (JE_UNLIKELY(tls_offset == TLS_OFFSET_UNINITIALIZED))
        tls_offset = tlsOffsetInitTsdTls();
    return reinterpret_cast<ThreadState *>(threadPointer() + tls_offset);
}

/// jemalloc: jemalloc_tls_addr_tsd_initialized (JEMALLOC_TLS_ADDR(tsd_initialized))
JE_ALWAYS_INLINE bool * tlsAddrTsdInitialized()
{
    intptr_t tls_offset = tls_offset_tsd_initialized.load(std::memory_order_relaxed);
    if (JE_UNLIKELY(tls_offset == TLS_OFFSET_UNINITIALIZED))
        tls_offset = tlsOffsetInitTsdInitialized();
    return reinterpret_cast<bool *>(threadPointer() + tls_offset);
}

#else

/// A `noinline` accessor returning the address laundered through an empty asm with a memory clobber: one real call
/// per access. jemalloc: JEMALLOC_TLS_ADDR (other GNU targets)
JE_NOINLINE ThreadState * tlsAddrTsdTls();
JE_NOINLINE bool * tlsAddrTsdInitialized();

#endif

}

/// --- TSD implementations ---------------------------------------------------------------------------------------

/// Linux: a `thread_local` + a pthread key whose destructor (`ThreadState::cleanup`) runs at thread exit.
/// jemalloc: tsd_tls.h
struct TsdTls
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
    static void boot1() {}

    /// jemalloc: tsd_get
    static JE_ALWAYS_INLINE ThreadState * get(bool /*init*/) { return tsd_detail::tlsAddrTsdTls(); }
    /// Arms the destructor. jemalloc: tsd_set
    static void set(ThreadState * val);
};

/// FreeBSD: `thread_local` + `tsd_initialized`; libthr calls `_malloc_thread_cleanup` at thread exit.
/// jemalloc: tsd_malloc_thread_cleanup.h
struct TsdMallocThreadCleanup
{
    static bool is_booted;
    static constexpr bool get_allocates = false;

    /// jemalloc: tsd_cleanup_wrapper. Returns true if it must be called again.
    static bool cleanupWrapper();

    static bool boot0();
    static void boot1() {}

    static JE_ALWAYS_INLINE ThreadState * get(bool /*init*/) { return tsd_detail::tlsAddrTsdTls(); }
    static void set(ThreadState * val);
};

/// The maximum number of cleanup functions registered with `mallocTsdCleanupRegister`.
/// jemalloc: MALLOC_TSD_CLEANUPS_MAX
inline constexpr unsigned MALLOC_TSD_CLEANUPS_MAX = 4;

/// jemalloc: malloc_tsd_cleanup_t
using MallocTsdCleanup = bool (*)();

/// jemalloc: _malloc_tsd_cleanup_register
void mallocTsdCleanupRegister(MallocTsdCleanup f);

/// Runs the registered cleanups until none of them asks to be run again. Exported as `_malloc_thread_cleanup` on
/// FreeBSD.
/// jemalloc: _malloc_thread_cleanup
void mallocThreadCleanup();

/// Darwin: the TSD lives in a heap-allocated wrapper found with `pthread_getspecific`.
/// jemalloc: tsd_generic.h
struct TsdGeneric
{
    /// jemalloc: tsd_wrapper_t
    struct Wrapper
    {
        bool initialized = false;
        ThreadState val;
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
    static JE_NOINLINE Wrapper * wrapperGetSlow();

    /// jemalloc: tsd_wrapper_get
    static JE_ALWAYS_INLINE Wrapper * wrapperGet(bool init)
    {
        if (JE_UNLIKELY(!is_booted))
            return &boot_wrapper;

        Wrapper * wrapper = static_cast<Wrapper *>(pthread_getspecific(key));
        if (init && JE_UNLIKELY(wrapper == nullptr))
            wrapper = wrapperGetSlow();
        return wrapper;
    }

    static bool boot0();
    static void boot1();

    static JE_ALWAYS_INLINE ThreadState * get(bool init)
    {
        JE_ASSERT(is_booted);
        Wrapper * wrapper = wrapperGet(init);
        if (!init && wrapper == nullptr)
            return nullptr;
        return &wrapper->val;
    }

    static void set(ThreadState * val);
};

/// The TSD implementation of this platform.
using Tsd = std::conditional_t<
    config::tsd_impl == TsdImpl::Tls,
    TsdTls,
    std::conditional_t<config::tsd_impl == TsdImpl::MallocThreadCleanup, TsdMallocThreadCleanup, TsdGeneric>>;

JE_ALWAYS_INLINE bool ThreadState::booted()
{
    return Tsd::is_booted;
}

JE_ALWAYS_INLINE ThreadState * ThreadState::fetchImpl(bool init, bool minimal)
{
    ThreadState * tsd = Tsd::get(init);

    if (!init && Tsd::get_allocates && tsd == nullptr)
        return nullptr;
    JE_ASSERT(tsd != nullptr);

    if (JE_UNLIKELY(tsd->stateGet() != tsd_state_nominal))
        return &tsd->fetchSlow(minimal);
    JE_ASSERT(tsd->fast());
    tsd->assertFast();

    return tsd;
}

JE_ALWAYS_INLINE ThreadState * ThreadState::tsdnFetch()
{
    if (!booted())
        return nullptr;
    return fetchImpl(false, false);
}

/// `arena` is the current context; reentry from arena 0 is not allowed (jemalloc asserts it in debug builds only;
/// the assertion needs the arena table and is not ported).
/// jemalloc: pre_reentrancy
JE_ALWAYS_INLINE void preReentrancy(ThreadState & tsd, Arena * /*arena*/)
{
    tsd.preReentrancyRaw();
}

/// jemalloc: post_reentrancy
JE_ALWAYS_INLINE void postReentrancy(ThreadState & tsd)
{
    tsd.postReentrancyRaw();
}

/// --- Hooks of other modules, called by the TSD life cycle --------------------------------------------------------
/// Defined by the owning modules: ThreadCache.cpp, Arenas.cpp, Prof.cpp.

/// Sets `tcache_enabled` from `opt.tcache` and initializes the automatic tcache. Returns true on error.
/// jemalloc: tsd_tcache_enabled_data_init (`tcache.c`)
bool tcacheTsdDataInit(ThreadState & tsd);
/// jemalloc: tcache_cleanup (`tcache.c`)
void tcacheCleanup(ThreadState & tsd);
/// jemalloc: arena_cleanup (`jemalloc.c`; defined in Arenas.cpp)
void arenaCleanup(ThreadState & tsd);
/// jemalloc: iarena_cleanup (`jemalloc.c`; defined in Arenas.cpp)
void iarenaCleanup(ThreadState & tsd);
/// jemalloc: prof_tdata_cleanup (`prof_data.c`)
void profTdataCleanup(ThreadState & tsd);
/// Internal allocation from arena 0 (used by `TsdGeneric` for the wrappers).
/// jemalloc: a0malloc, a0dalloc (`jemalloc.c`; defined in Arenas.cpp)
void * a0malloc(size_t size);
void a0dalloc(void * ptr);

}
