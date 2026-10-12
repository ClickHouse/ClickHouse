#pragma once

/// Heap profiling (jemalloc: `prof.c`, `prof_data.c`, `prof_sys.c`, `prof_recent.c`, `prof_stats.c` and the
/// `prof_*.h` headers). `prof_log.c` is dropped.
///
/// The implementation is split like jemalloc's:
///     Profiling.cpp        options-related state, the sampling event, the entry points used by the rest of the allocator,
///                     boot and fork (`prof.c`);
///     ProfilingData.cpp    the core data structures: `backtrace_to_global_context`, the tdata tree, tctx/gctx/tdata life cycles, the
///                     aggregation and the formatting of heap dumps (`prof_data.c`);
///     ProfilingSystem.cpp     backtraces, thread names, dump files and their names, `MAPPED_LIBRARIES` (`prof_sys.c`);
///     ProfilingRecent.cpp  the record of recent sampled allocations (`prof_recent.c`);
///     ProfilingStats.cpp   per size class statistics of sampled allocations (`prof_stats.c`).
///
/// The inline logic used on the allocation paths is in ProfilingHooks.h (`prof_inlines.h`).

#include <allocator/Arena.h>
#include <allocator/Common.h>
#include <allocator/CuckooHash.h>
#include <allocator/Extent.h>
#include <allocator/Format.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mutex.h>
#include <allocator/Nanoseconds.h>
#include <allocator/ProfilingTree.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace jemalloc
{

class Base;
class ProfilingGlobalContext;
class ProfilingThreadData;

/// --- Constants (prof_types.h) -----------------------------------------------------------------------------------

/// Initial hash table size. jemalloc: PROF_CKH_MINITEMS
inline constexpr size_t PROFILING_CUCKOO_HASH_MIN_ITEMS = 64;
/// Size of memory buffer to use when writing dump files (jemalloc uses 16 with `JEMALLOC_DEBUG`, which is never
/// defined in ClickHouse builds; `ALLOCATOR_DEBUG` only enables assertions). jemalloc: PROF_DUMP_BUFSIZE
inline constexpr size_t PROFILING_DUMP_BUF_SIZE = 65536;
/// Size of stack-allocated buffer used by `profilingDumpPrintf`. jemalloc: PROF_PRINTF_BUFSIZE
inline constexpr size_t PROFILING_PRINTF_BUF_SIZE = 128;
/// Number of mutexes shared among all gctx's. jemalloc: PROF_NCTX_LOCKS
inline constexpr unsigned PROFILING_NUM_CONTEXT_LOCKS = 1024;
/// Number of mutexes shared among all tdata's. jemalloc: PROF_NTDATA_LOCKS
inline constexpr unsigned PROFILING_NUM_THREAD_DATA_LOCKS = 256;
/// Thread name storage size limit. jemalloc: PROF_THREAD_NAME_MAX_LEN
inline constexpr size_t PROFILING_THREAD_NAME_MAX_LEN = 16;

/// --- Hooks (prof_hook.h) ------------------------------------------------------------------------------------------

/// jemalloc: prof_backtrace_hook_t
using ProfilingBacktraceHook = void (*)(void ** vector, unsigned * len, unsigned max_len);
/// A callback hook that notifies about a recently dumped heap profile. jemalloc: prof_dump_hook_t
using ProfilingDumpHook = void (*)(const char * filename);
/// ptr, size, backtrace vector, backtrace vector length, usize. jemalloc: prof_sample_hook_t
using ProfilingSampleHook = void (*)(const void * ptr, size_t size, void ** backtrace, unsigned backtrace_length, size_t usable_size);
/// ptr, usize. jemalloc: prof_sample_free_hook_t
using ProfilingSampleFreeHook = void (*)(const void * ptr, size_t usable_size);

/// jemalloc: prof_backtrace_hook_set, prof_backtrace_hook_get, prof_dump_hook_set, ... (release / acquire)
void profilingBacktraceHookSet(ProfilingBacktraceHook hook);
ProfilingBacktraceHook profilingBacktraceHookGet();
void profilingDumpHookSet(ProfilingDumpHook hook);
ProfilingDumpHook profilingDumpHookGet();
void profilingSampleHookSet(ProfilingSampleHook hook);
ProfilingSampleHook profilingSampleHookGet();
void profilingSampleFreeHookSet(ProfilingSampleFreeHook hook);
ProfilingSampleFreeHook profilingSampleFreeHookGet();

/// --- Data structures (prof_structs.h) ---------------------------------------------------------------------------

/// jemalloc: prof_bt_t
struct ProfilingBacktrace
{
    /// Backtrace, stored as len program counters.
    void ** vector;
    unsigned len;
};

/// jemalloc: prof_cnt_t
struct ProfilingCounters
{
    uint64_t current_objects;
    uint64_t current_objects_shifted_unbiased;
    uint64_t current_bytes;
    uint64_t current_bytes_unbiased;
    uint64_t accumulated_objects;
    uint64_t accumulated_objects_shifted_unbiased;
    uint64_t accumulated_bytes;
    uint64_t accumulated_bytes_unbiased;
};

static_assert(sizeof(ProfilingCounters) == 64);

/// jemalloc: prof_tctx_state_t
enum ProfilingThreadContextState : unsigned
{
    profiling_thread_context_state_initializing,
    profiling_thread_context_state_nominal,
    profiling_thread_context_state_dumping,
    profiling_thread_context_state_purgatory, /// Dumper must finish destroying.
};

/// The counters of one (thread, backtrace) pair. jemalloc: prof_tctx_t
class ProfilingThreadContext
{
public:
    /// Thread data for thread that performed the allocation.
    ProfilingThreadData * thread_data;
    /// Copy of tdata->thr_{uid,discrim}, necessary because tdata may be defunct during teardown.
    uint64_t thread_uid;
    uint64_t thread_discriminator;
    /// Reference count of how many times this tctx object is referenced in recent allocation / deallocation
    /// records, protected by tdata->lock.
    uint64_t recent_count;
    /// Profiling counters, protected by tdata->lock.
    ProfilingCounters counts;
    /// Associated global context.
    ProfilingGlobalContext * global_context;
    /// UID that distinguishes multiple tctx's created by the same thread, but coexisting in gctx->tctxs.
    uint64_t thread_context_uid;
    /// Linkage into gctx's tctxs.
    ProfilingTreeLink<ProfilingThreadContext> thread_context_link;
    /// True during prof_alloc_prep()..prof_malloc_sample_object(), prevents sample vs destroy race.
    bool prepared;
    /// Current dump-related state, protected by gctx->lock.
    ProfilingThreadContextState state;
    /// Copy of cnts snapshotted during early dump phase, protected by dump_mtx.
    ProfilingCounters dump_counts;
};

static_assert(sizeof(ProfilingThreadContext) == 200, "Must have the size of prof_tctx_t");

/// jemalloc: prof_tctx_comp
int profilingThreadContextCompare(const ProfilingThreadContext * a, const ProfilingThreadContext * b);
/// jemalloc: prof_tctx_tree_t
using ProfilingThreadContextTree
    = ProfilingTree<ProfilingThreadContext, &ProfilingThreadContext::thread_context_link, profilingThreadContextCompare>;

/// The counters of one backtrace. jemalloc: prof_gctx_t
class ProfilingGlobalContext
{
public:
    /// Protects nlimbo, cnt_summed, and tctxs.
    Mutex * lock;
    /// Number of threads that currently cause this gctx to be in a state of limbo. nlimbo must be 1 (single
    /// destroyer) in order to safely destroy the gctx.
    unsigned num_limbo;
    /// Tree of profile counters, one for each thread that has allocated in this context.
    ProfilingThreadContextTree thread_contexts;
    /// Linkage for tree of contexts to be dumped.
    ProfilingTreeLink<ProfilingGlobalContext> dump_link;
    /// Temporary storage for summation during dump.
    ProfilingCounters count_summed;
    /// ClickHouse fork: the live sampled allocations whose backtrace resolved to this gctx, linked through the
    /// extents' `e_profiling_fragmentation_link`. Protected by lock.
    ExtentListFragmentation fragmentation_objects;
    /// Associated backtrace.
    ProfilingBacktrace backtrace;
    /// Backtrace vector, variable size, referred to by bt.
    void * vector[1];
};

static_assert(offsetof(ProfilingGlobalContext, vector) == 128, "Must have the layout of prof_gctx_t");

/// jemalloc: prof_gctx_comp (memcmp on the raw bytes of the backtraces, then the length)
int profilingGlobalContextCompare(const ProfilingGlobalContext * a, const ProfilingGlobalContext * b);
/// jemalloc: prof_gctx_tree_t
using ProfilingGlobalContextTree = ProfilingTree<ProfilingGlobalContext, &ProfilingGlobalContext::dump_link, profilingGlobalContextCompare>;

/// The table allocator of the profiler's cuckoo hashes: `ipallocztm(tsdn, usize, CACHELINE, zero = true, NULL,
/// is_internal = true, arena_ichoose(tsd, NULL))` / `idalloctm(tsdn, ptr, NULL, NULL, true, true)`.
struct ProfilingCuckooHashAllocator
{
    static void * allocate(ThreadState & thread_state, size_t usable_size, size_t alignment);
    static void deallocate(ThreadState & thread_state, void * ptr);
};

using ProfilingCuckooHash = CuckooHash<ProfilingCuckooHashAllocator>;

/// Per-thread profiling data. jemalloc: prof_tdata_t
class ProfilingThreadData
{
public:
    Mutex * lock;
    /// Monotonically increasing unique thread identifier.
    uint64_t thread_uid;
    /// Monotonically increasing discriminator among tdata structures associated with the same thr_uid.
    uint64_t thread_discriminator;
    ProfilingTreeLink<ProfilingThreadData> thread_data_link;
    /// Counter used to initialize ProfilingThreadContext::tctx_uid.
    uint64_t thread_context_uid_next;
    /// Hash of (ProfilingBacktrace *) -> (ProfilingThreadContext *).
    ProfilingCuckooHash backtrace_to_thread_context;
    /// Included in heap profile dumps if has content.
    char thread_name[PROFILING_THREAD_NAME_MAX_LEN];
    /// State used to avoid dumping while operating on prof internals.
    bool enqueued;
    bool enqueued_interval_dump;
    bool enqueued_growth_dump;
    /// Set to true during an early dump phase for tdata's which are currently being dumped.
    bool dumping;
    /// True if profiling is active for this tdata's thread (`thread.prof.active`).
    bool active;
    bool attached;
    bool expired;
    /// Temporary storage for summation during dump.
    ProfilingCounters count_summed;
    /// Backtrace vector, used for calls to `profilingBacktrace`.
    void ** vector;
};

static_assert(sizeof(ProfilingThreadData) == 192, "Must have the size of prof_tdata_t");

/// jemalloc: prof_tdata_comp
int profilingThreadDataCompare(const ProfilingThreadData * a, const ProfilingThreadData * b);
/// jemalloc: prof_tdata_tree_t
using ProfilingThreadDataTree = ProfilingTree<ProfilingThreadData, &ProfilingThreadData::thread_data_link, profilingThreadDataCompare>;

/// A record of a recent sampled allocation. jemalloc: prof_recent_t
class ProfilingRecent
{
public:
    Nanoseconds alloc_time;
    Nanoseconds deallocation_time;
    RingLink<ProfilingRecent> link;
    size_t size;
    size_t usable_size;
    /// Null means the allocation has been freed. Atomic (acquire / release).
    std::atomic<Extent *> alloc_extent;
    ProfilingThreadContext * alloc_thread_context;
    ProfilingThreadContext * deallocation_thread_context;
};

static_assert(sizeof(ProfilingRecent) == 72, "Must have the size of prof_recent_t");

/// jemalloc: prof_recent_list_t
using ProfilingRecentList = IntrusiveList<ProfilingRecent, &ProfilingRecent::link>;

/// jemalloc: prof_stats_t
struct ProfilingStats
{
    uint64_t request_sum;
    uint64_t count;
};

/// --- Global state ------------------------------------------------------------------------------------------------

/// `profiling_active_state`, `log2_profiling_sample`, `profiling_interval` are declared in ProfilingHooks.h.

/// Initialized to `opt.prof_gdump`; accessed via `profilingGrowthDump{Get,Set}{Unlocked,}`. jemalloc: prof_gdump_val
extern constinit std::atomic<bool> profiling_growth_dump_value;
/// Do not dump any profiles until bootstrapping is complete. jemalloc: prof_booted
extern constinit bool profiling_booted;

/// jemalloc: bt2gctx_mtx, tdatas_mtx, prof_dump_mtx (`prof_data.c`)
extern constinit Mutex backtrace_to_global_context_mutex;
extern constinit Mutex all_thread_data_mutex;
extern constinit Mutex profiling_dump_mutex;
/// Tables of mutexes shared among gctx's / tdata's (base-allocated by `profilingBoot2`).
/// jemalloc: gctx_locks, tdata_locks
extern constinit Mutex * global_context_locks;
extern constinit Mutex * thread_data_locks;
/// jemalloc: prof_unbiased_sz, prof_shifted_unbiased_cnt
extern constinit size_t profiling_unbiased_size[SIZE_CLASS_NUM_SIZES];
extern constinit size_t profiling_shifted_unbiased_count[SIZE_CLASS_NUM_SIZES];

/// jemalloc: prof_dump_filename_mtx, prof_base (`prof_sys.c`)
extern constinit Mutex profiling_dump_filename_mutex;
extern constinit Base * profiling_base;

/// jemalloc: prof_recent_alloc_mtx, prof_recent_dump_mtx (`prof_recent.c`)
extern constinit Mutex profiling_recent_alloc_mutex;
extern constinit Mutex profiling_recent_dump_mutex;

/// jemalloc: prof_stats_mtx (`prof_stats.c`)
extern constinit Mutex profiling_stats_mutex;

/// --- Inline functions (prof_inlines.h) ---------------------------------------------------------------------------

/// jemalloc: prof_gdump_get_unlocked
ALLOCATOR_ALWAYS_INLINE bool profilingGrowthDumpGetUnlocked()
{
    /// No locking is used when reading `profiling_growth_dump_value` in the fast path, so there are no guarantees regarding how
    /// long it will take for all threads to notice state changes.
    return profiling_growth_dump_value.load(std::memory_order_relaxed);
}

/// jemalloc: prof_thread_name_assert
ALLOCATOR_ALWAYS_INLINE void profilingThreadNameAssert(const ProfilingThreadData * thread_data)
{
    if constexpr (config::debug)
    {
        bool terminated = false;
        for (size_t i = 0; i < PROFILING_THREAD_NAME_MAX_LEN; ++i)
        {
            if (thread_data->thread_name[i] == '\0')
                terminated = true;
        }
        ALLOCATOR_ASSERT(terminated);
    }
    (void)thread_data;
}

/// jemalloc: prof_tdata_init, prof_tdata_reinit
ProfilingThreadData * profilingThreadDataInit(ThreadState & thread_state);
ProfilingThreadData * profilingThreadDataReinit(ThreadState & thread_state, ProfilingThreadData * thread_data);

/// jemalloc: prof_tdata_get
ALLOCATOR_ALWAYS_INLINE ProfilingThreadData * profilingThreadDataGet(ThreadState & thread_state, bool create)
{
    ProfilingThreadData * thread_data = thread_state.profiling_thread_data;
    if (create)
    {
        ALLOCATOR_ASSERT(thread_state.reentrancyLevel() == 0);
        if (ALLOCATOR_UNLIKELY(thread_data == nullptr))
        {
            if (thread_state.nominal())
            {
                thread_data = profilingThreadDataInit(thread_state);
                thread_state.profiling_thread_data = thread_data;
            }
        }
        else if (ALLOCATOR_UNLIKELY(thread_data->expired))
        {
            thread_data = profilingThreadDataReinit(thread_state, thread_data);
            thread_state.profiling_thread_data = thread_data;
        }
        ALLOCATOR_ASSERT(thread_data == nullptr || thread_data->attached);
    }
    if (thread_data != nullptr)
        profilingThreadNameAssert(thread_data);
    return thread_data;
}

/// jemalloc: prof_thread_name_clear (`prof_data.h`)
ALLOCATOR_ALWAYS_INLINE void profilingThreadNameClear(ProfilingThreadData * thread_data)
{
    thread_data->thread_name[0] = '\0';
}

/// jemalloc: prof_thread_name_empty (`prof_data.h`)
ALLOCATOR_ALWAYS_INLINE bool profilingThreadNameEmpty(const ProfilingThreadData * thread_data)
{
    profilingThreadNameAssert(thread_data);
    return thread_data->thread_name[0] == '\0';
}

/// --- prof.c --------------------------------------------------------------------------------------------------------

/// Dump a heap profile when the total virtual memory reaches a new high (`prof.gdump`).
/// jemalloc: prof_gdump
void profilingGrowthDump(ThreadState * thread_state);
/// An interval-triggered dump. jemalloc: prof_idump
void profilingIntervalDump(ThreadState * thread_state);
/// `prof.dump`; returns true on error. jemalloc: prof_mdump
bool profilingManualDump(ThreadState & thread_state, const char * filename);

/// jemalloc: prof_active_get, prof_active_set (returns the old value)
bool profilingActiveGet(ThreadState * thread_state);
bool profilingActiveSet(ThreadState * thread_state, bool active);
/// jemalloc: prof_thread_name_get, prof_thread_name_set (returns an errno value)
const char * profilingThreadNameGet(ThreadState & thread_state);
int profilingThreadNameSet(ThreadState & thread_state, const char * thread_name);
/// jemalloc: prof_thread_active_get, prof_thread_active_set (returns true on error)
bool profilingThreadActiveGet(ThreadState & thread_state);
bool profilingThreadActiveSet(ThreadState & thread_state, bool active);
/// jemalloc: prof_thread_active_init_get, prof_thread_active_init_set (returns the old value)
bool profilingThreadActiveInitGet(ThreadState * thread_state);
bool profilingThreadActiveInitSet(ThreadState * thread_state, bool active_init);
/// jemalloc: prof_gdump_get, prof_gdump_set (returns the old value)
bool profilingGrowthDumpGet(ThreadState * thread_state);
bool profilingGrowthDumpSet(ThreadState * thread_state, bool growth_dump);

/// --- prof_data.c ---------------------------------------------------------------------------------------------------

/// Returns true on error. jemalloc: prof_data_init
bool profilingDataInit(ThreadState & thread_state);
/// Track / untrack a live sampled allocation on its gctx's `fragmentation_objects` list (ClickHouse fork).
/// jemalloc: prof_frag_track, prof_frag_untrack
void profilingFragmentationTrack(ThreadState & thread_state, Extent * extent, ProfilingThreadContext * thread_context);
/// `profilingFragmentationUntrack` is declared in Arena.h.
/// jemalloc: prof_lookup
ProfilingThreadContext * profilingLookup(ThreadState & thread_state, ProfilingBacktrace * backtrace);
/// jemalloc: prof_thread_name_set_impl
int profilingThreadNameSetImpl(ThreadState & thread_state, const char * thread_name);
/// jemalloc: prof_unbias_map_init
void profilingUnbiasMapInit();
/// Requires `profiling_dump_mutex`. jemalloc: prof_dump_impl
void profilingDumpImpl(
    ThreadState & thread_state,
    WriteCallback * profiling_dump_write,
    void * callback_argument,
    ProfilingThreadData * thread_data,
    bool leak_check);
/// jemalloc: prof_bt_hash, prof_bt_keycomp
void profilingBacktraceHash(const void * key, size_t result_hash[2]);
bool profilingBacktraceKeyCompare(const void * k1, const void * k2);
/// jemalloc: prof_tdata_init_impl
ProfilingThreadData * profilingThreadDataInitImpl(
    ThreadState & thread_state, uint64_t thread_uid, uint64_t thread_discriminator, const char * thread_name, bool active);
/// jemalloc: prof_tdata_detach
void profilingThreadDataDetach(ThreadState & thread_state, ProfilingThreadData * thread_data);
/// `prof.reset`. jemalloc: prof_reset
void profilingReset(ThreadState & thread_state, size_t log2_sample);
/// Requires `thread_context->thread_data->lock`, which is released. jemalloc: prof_tctx_try_destroy
void profilingThreadContextTryDestroy(ThreadState & thread_state, ProfilingThreadContext * thread_context);

/// Internal allocations of the profiler (`is_internal`, no tcache): `arena_get(TSDN_NULL, 0, true)` (tdata, gctx),
/// `arena_ichoose(thread_state, NULL)` (tctx), `arena_get(thread_state, 0, false)` (recent records);
/// `internalDeallocateFull(..., true, true)`.
void * profilingAllocArena0(ThreadState & thread_state, size_t size, bool init_if_missing);
void * profilingAllocInternalArena(ThreadState & thread_state, size_t size);
void profilingInternalDeallocate(ThreadState * thread_state, void * ptr);

/// The number of tdatas / backtraces (tests). jemalloc: prof_tdata_count, prof_bt_count
size_t profilingThreadDataCount();
size_t profilingBacktraceCount();

/// --- prof_sys.c ----------------------------------------------------------------------------------------------------

/// jemalloc: bt_init
void backtraceInit(ProfilingBacktrace * backtrace, void ** vector);
/// Calls the backtrace hook inside a reentrancy section. jemalloc: prof_backtrace
void profilingBacktrace(ThreadState & thread_state, ProfilingBacktrace * backtrace);
/// The default backtrace hook (`unw_backtrace`). jemalloc: prof_backtrace_impl
void profilingBacktraceImpl(void ** vector, unsigned * len, unsigned max_len);
/// jemalloc: prof_hooks_init, prof_unwind_init
void profilingHooksInit();
void profilingUnwindInit();
/// jemalloc: prof_sys_thread_name_fetch
void profilingSystemThreadNameFetch(ThreadState & thread_state);
/// jemalloc: prof_getpid
int profilingGetPID();
/// Under `mallctl_mutex`; returns true on error. jemalloc: prof_prefix_set
bool profilingPrefixSet(ThreadState * thread_state, const char * prefix);
/// jemalloc: prof_fdump_impl, prof_idump_impl, prof_mdump_impl, prof_gdump_impl
void profilingFinalDumpImpl(ThreadState & thread_state);
void profilingIntervalDumpImpl(ThreadState & thread_state);
bool profilingManualDumpImpl(ThreadState & thread_state, const char * filename);
void profilingGrowthDumpImpl(ThreadState & thread_state);

/// --- prof_recent.c -------------------------------------------------------------------------------------------------

/// Requires `thread_context->thread_data->lock`. jemalloc: prof_recent_alloc_prepare
bool profilingRecentAllocPrepare(ThreadState & thread_state, ProfilingThreadContext * thread_context);
/// jemalloc: prof_recent_alloc
void profilingRecentAlloc(ThreadState & thread_state, Extent * extent, size_t size, size_t usable_size);
/// `profilingRecentAllocReset` is declared in Arena.h.
/// jemalloc: prof_recent_alloc_max_ctl_read, prof_recent_alloc_max_ctl_write (returns the old value)
ssize_t profilingRecentAllocMaxMallctlRead();
ssize_t profilingRecentAllocMaxMallctlWrite(ThreadState & thread_state, ssize_t max);
/// jemalloc: prof_recent_alloc_dump
void profilingRecentAllocDump(ThreadState & thread_state, WriteCallback * write_callback, void * callback_argument);
/// Returns true on error. jemalloc: prof_recent_init
bool profilingRecentInit();

/// --- prof_stats.c --------------------------------------------------------------------------------------------------

/// jemalloc: prof_stats_inc, prof_stats_dec, prof_stats_get_live, prof_stats_get_accum
void profilingStatsIncrement(ThreadState & thread_state, SizeClassIdx idx, size_t size);
void profilingStatsDecrement(ThreadState & thread_state, SizeClassIdx idx, size_t size);
void profilingStatsGetLive(ThreadState & thread_state, SizeClassIdx idx, ProfilingStats * stats);
void profilingStatsGetAccumulated(ThreadState & thread_state, SizeClassIdx idx, ProfilingStats * stats);

}
