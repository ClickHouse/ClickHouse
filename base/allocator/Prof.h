#pragma once

/// Heap profiling (jemalloc: `prof.c`, `prof_data.c`, `prof_sys.c`, `prof_recent.c`, `prof_stats.c` and the
/// `prof_*.h` headers). `prof_log.c` is dropped.
///
/// The implementation is split like jemalloc's:
///     Prof.cpp        options-related state, the sampling event, the entry points used by the rest of the allocator,
///                     boot and fork (`prof.c`);
///     ProfData.cpp    the core data structures: `bt2gctx`, the tdata tree, tctx/gctx/tdata life cycles, the
///                     aggregation and the formatting of heap dumps (`prof_data.c`);
///     ProfSys.cpp     backtraces, thread names, dump files and their names, `MAPPED_LIBRARIES` (`prof_sys.c`);
///     ProfRecent.cpp  the record of recent sampled allocations (`prof_recent.c`);
///     ProfStats.cpp   per size class statistics of sampled allocations (`prof_stats.c`).
///
/// The inline logic used on the allocation paths is in ProfHooks.h (`prof_inlines.h`).

#include <allocator/Arena.h>
#include <allocator/Common.h>
#include <allocator/CuckooHash.h>
#include <allocator/Extent.h>
#include <allocator/Format.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Mutex.h>
#include <allocator/NsTime.h>
#include <allocator/ProfTree.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace jemalloc
{

class Base;
class ProfGlobalContext;
class ProfThreadData;

/// --- Constants (prof_types.h) -----------------------------------------------------------------------------------

/// Initial hash table size. jemalloc: PROF_CKH_MINITEMS
inline constexpr size_t PROF_CKH_MINITEMS = 64;
/// Size of memory buffer to use when writing dump files (jemalloc uses 16 with `JEMALLOC_DEBUG`, which is never
/// defined in ClickHouse builds; `ALLOCATOR_DEBUG` only enables assertions). jemalloc: PROF_DUMP_BUFSIZE
inline constexpr size_t PROF_DUMP_BUFSIZE = 65536;
/// Size of stack-allocated buffer used by `profDumpPrintf`. jemalloc: PROF_PRINTF_BUFSIZE
inline constexpr size_t PROF_PRINTF_BUFSIZE = 128;
/// Number of mutexes shared among all gctx's. jemalloc: PROF_NCTX_LOCKS
inline constexpr unsigned PROF_NCTX_LOCKS = 1024;
/// Number of mutexes shared among all tdata's. jemalloc: PROF_NTDATA_LOCKS
inline constexpr unsigned PROF_NTDATA_LOCKS = 256;
/// Thread name storage size limit. jemalloc: PROF_THREAD_NAME_MAX_LEN
inline constexpr size_t PROF_THREAD_NAME_MAX_LEN = 16;

/// --- Hooks (prof_hook.h) ------------------------------------------------------------------------------------------

/// jemalloc: prof_backtrace_hook_t
using ProfBacktraceHook = void (*)(void ** vec, unsigned * len, unsigned max_len);
/// A callback hook that notifies about a recently dumped heap profile. jemalloc: prof_dump_hook_t
using ProfDumpHook = void (*)(const char * filename);
/// ptr, size, backtrace vector, backtrace vector length, usize. jemalloc: prof_sample_hook_t
using ProfSampleHook = void (*)(const void * ptr, size_t size, void ** backtrace, unsigned backtrace_length, size_t usize);
/// ptr, usize. jemalloc: prof_sample_free_hook_t
using ProfSampleFreeHook = void (*)(const void * ptr, size_t usize);

/// jemalloc: prof_backtrace_hook_set, prof_backtrace_hook_get, prof_dump_hook_set, ... (release / acquire)
void profBacktraceHookSet(ProfBacktraceHook hook);
ProfBacktraceHook profBacktraceHookGet();
void profDumpHookSet(ProfDumpHook hook);
ProfDumpHook profDumpHookGet();
void profSampleHookSet(ProfSampleHook hook);
ProfSampleHook profSampleHookGet();
void profSampleFreeHookSet(ProfSampleFreeHook hook);
ProfSampleFreeHook profSampleFreeHookGet();

/// --- Data structures (prof_structs.h) ---------------------------------------------------------------------------

/// jemalloc: prof_bt_t
struct ProfBacktrace
{
    /// Backtrace, stored as len program counters.
    void ** vec;
    unsigned len;
};

/// jemalloc: prof_cnt_t
struct ProfCounters
{
    uint64_t curobjs;
    uint64_t curobjs_shifted_unbiased;
    uint64_t curbytes;
    uint64_t curbytes_unbiased;
    uint64_t accumobjs;
    uint64_t accumobjs_shifted_unbiased;
    uint64_t accumbytes;
    uint64_t accumbytes_unbiased;
};

static_assert(sizeof(ProfCounters) == 64);

/// jemalloc: prof_tctx_state_t
enum ProfTctxState : unsigned
{
    prof_tctx_state_initializing,
    prof_tctx_state_nominal,
    prof_tctx_state_dumping,
    prof_tctx_state_purgatory, /// Dumper must finish destroying.
};

/// The counters of one (thread, backtrace) pair. jemalloc: prof_tctx_t
class ProfThreadContext
{
public:
    /// Thread data for thread that performed the allocation.
    ProfThreadData * tdata;
    /// Copy of tdata->thr_{uid,discrim}, necessary because tdata may be defunct during teardown.
    uint64_t thr_uid;
    uint64_t thr_discrim;
    /// Reference count of how many times this tctx object is referenced in recent allocation / deallocation
    /// records, protected by tdata->lock.
    uint64_t recent_count;
    /// Profiling counters, protected by tdata->lock.
    ProfCounters cnts;
    /// Associated global context.
    ProfGlobalContext * gctx;
    /// UID that distinguishes multiple tctx's created by the same thread, but coexisting in gctx->tctxs.
    uint64_t tctx_uid;
    /// Linkage into gctx's tctxs.
    ProfTreeLink<ProfThreadContext> tctx_link;
    /// True during prof_alloc_prep()..prof_malloc_sample_object(), prevents sample vs destroy race.
    bool prepared;
    /// Current dump-related state, protected by gctx->lock.
    ProfTctxState state;
    /// Copy of cnts snapshotted during early dump phase, protected by dump_mtx.
    ProfCounters dump_cnts;
};

static_assert(sizeof(ProfThreadContext) == 200, "Must have the size of prof_tctx_t");

/// jemalloc: prof_tctx_comp
int profTctxCompare(const ProfThreadContext * a, const ProfThreadContext * b);
/// jemalloc: prof_tctx_tree_t
using ProfTctxTree = ProfTree<ProfThreadContext, &ProfThreadContext::tctx_link, profTctxCompare>;

/// The counters of one backtrace. jemalloc: prof_gctx_t
class ProfGlobalContext
{
public:
    /// Protects nlimbo, cnt_summed, and tctxs.
    Mutex * lock;
    /// Number of threads that currently cause this gctx to be in a state of limbo. nlimbo must be 1 (single
    /// destroyer) in order to safely destroy the gctx.
    unsigned nlimbo;
    /// Tree of profile counters, one for each thread that has allocated in this context.
    ProfTctxTree tctxs;
    /// Linkage for tree of contexts to be dumped.
    ProfTreeLink<ProfGlobalContext> dump_link;
    /// Temporary storage for summation during dump.
    ProfCounters cnt_summed;
    /// ClickHouse fork: the live sampled allocations whose backtrace resolved to this gctx, linked through the
    /// extents' `e_prof_frag_link`. Protected by lock.
    ExtentListFrag frag_objs;
    /// Associated backtrace.
    ProfBacktrace bt;
    /// Backtrace vector, variable size, referred to by bt.
    void * vec[1];
};

static_assert(offsetof(ProfGlobalContext, vec) == 128, "Must have the layout of prof_gctx_t");

/// jemalloc: prof_gctx_comp (memcmp on the raw bytes of the backtraces, then the length)
int profGctxCompare(const ProfGlobalContext * a, const ProfGlobalContext * b);
/// jemalloc: prof_gctx_tree_t
using ProfGctxTree = ProfTree<ProfGlobalContext, &ProfGlobalContext::dump_link, profGctxCompare>;

/// The table allocator of the profiler's cuckoo hashes: `ipallocztm(tsdn, usize, CACHELINE, zero = true, NULL,
/// is_internal = true, arena_ichoose(tsd, NULL))` / `idalloctm(tsdn, ptr, NULL, NULL, true, true)`.
struct ProfCuckooHashAllocator
{
    static void * allocate(ThreadState & tsd, size_t usize, size_t alignment);
    static void deallocate(ThreadState & tsd, void * ptr);
};

using ProfCuckooHash = CuckooHash<ProfCuckooHashAllocator>;

/// Per-thread profiling data. jemalloc: prof_tdata_t
class ProfThreadData
{
public:
    Mutex * lock;
    /// Monotonically increasing unique thread identifier.
    uint64_t thr_uid;
    /// Monotonically increasing discriminator among tdata structures associated with the same thr_uid.
    uint64_t thr_discrim;
    ProfTreeLink<ProfThreadData> tdata_link;
    /// Counter used to initialize ProfThreadContext::tctx_uid.
    uint64_t tctx_uid_next;
    /// Hash of (ProfBacktrace *) -> (ProfThreadContext *).
    ProfCuckooHash bt2tctx;
    /// Included in heap profile dumps if has content.
    char thread_name[PROF_THREAD_NAME_MAX_LEN];
    /// State used to avoid dumping while operating on prof internals.
    bool enq;
    bool enq_idump;
    bool enq_gdump;
    /// Set to true during an early dump phase for tdata's which are currently being dumped.
    bool dumping;
    /// True if profiling is active for this tdata's thread (`thread.prof.active`).
    bool active;
    bool attached;
    bool expired;
    /// Temporary storage for summation during dump.
    ProfCounters cnt_summed;
    /// Backtrace vector, used for calls to `profBacktrace`.
    void ** vec;
};

static_assert(sizeof(ProfThreadData) == 192, "Must have the size of prof_tdata_t");

/// jemalloc: prof_tdata_comp
int profTdataCompare(const ProfThreadData * a, const ProfThreadData * b);
/// jemalloc: prof_tdata_tree_t
using ProfTdataTree = ProfTree<ProfThreadData, &ProfThreadData::tdata_link, profTdataCompare>;

/// A record of a recent sampled allocation. jemalloc: prof_recent_t
class ProfRecent
{
public:
    NsTime alloc_time;
    NsTime dalloc_time;
    RingLink<ProfRecent> link;
    size_t size;
    size_t usize;
    /// Null means the allocation has been freed. Atomic (acquire / release).
    std::atomic<Extent *> alloc_edata;
    ProfThreadContext * alloc_tctx;
    ProfThreadContext * dalloc_tctx;
};

static_assert(sizeof(ProfRecent) == 72, "Must have the size of prof_recent_t");

/// jemalloc: prof_recent_list_t
using ProfRecentList = IntrusiveList<ProfRecent, &ProfRecent::link>;

/// jemalloc: prof_stats_t
struct ProfStats
{
    uint64_t req_sum;
    uint64_t count;
};

/// --- Global state ------------------------------------------------------------------------------------------------

/// `prof_active_state`, `lg_prof_sample`, `prof_interval` are declared in ProfHooks.h.

/// Initialized to `opt.prof_gdump`; accessed via `profGdump{Get,Set}{Unlocked,}`. jemalloc: prof_gdump_val
extern constinit std::atomic<bool> prof_gdump_val;
/// Do not dump any profiles until bootstrapping is complete. jemalloc: prof_booted
extern constinit bool prof_booted;

/// jemalloc: bt2gctx_mtx, tdatas_mtx, prof_dump_mtx (`prof_data.c`)
extern constinit Mutex bt2gctx_mtx;
extern constinit Mutex tdatas_mtx;
extern constinit Mutex prof_dump_mtx;
/// Tables of mutexes shared among gctx's / tdata's (base-allocated by `profBoot2`).
/// jemalloc: gctx_locks, tdata_locks
extern constinit Mutex * gctx_locks;
extern constinit Mutex * tdata_locks;
/// jemalloc: prof_unbiased_sz, prof_shifted_unbiased_cnt
extern constinit size_t prof_unbiased_sz[SC_NSIZES];
extern constinit size_t prof_shifted_unbiased_cnt[SC_NSIZES];

/// jemalloc: prof_dump_filename_mtx, prof_base (`prof_sys.c`)
extern constinit Mutex prof_dump_filename_mtx;
extern constinit Base * prof_base;

/// jemalloc: prof_recent_alloc_mtx, prof_recent_dump_mtx (`prof_recent.c`)
extern constinit Mutex prof_recent_alloc_mtx;
extern constinit Mutex prof_recent_dump_mtx;

/// jemalloc: prof_stats_mtx (`prof_stats.c`)
extern constinit Mutex prof_stats_mtx;

/// --- Inline functions (prof_inlines.h) ---------------------------------------------------------------------------

/// jemalloc: prof_gdump_get_unlocked
JE_ALWAYS_INLINE bool profGdumpGetUnlocked()
{
    /// No locking is used when reading `prof_gdump_val` in the fast path, so there are no guarantees regarding how
    /// long it will take for all threads to notice state changes.
    return prof_gdump_val.load(std::memory_order_relaxed);
}

/// jemalloc: prof_thread_name_assert
JE_ALWAYS_INLINE void profThreadNameAssert(const ProfThreadData * tdata)
{
    if constexpr (config::debug)
    {
        bool terminated = false;
        for (size_t i = 0; i < PROF_THREAD_NAME_MAX_LEN; ++i)
        {
            if (tdata->thread_name[i] == '\0')
                terminated = true;
        }
        JE_ASSERT(terminated);
    }
    (void)tdata;
}

/// jemalloc: prof_tdata_init, prof_tdata_reinit
ProfThreadData * profTdataInit(ThreadState & tsd);
ProfThreadData * profTdataReinit(ThreadState & tsd, ProfThreadData * tdata);

/// jemalloc: prof_tdata_get
JE_ALWAYS_INLINE ProfThreadData * profTdataGet(ThreadState & tsd, bool create)
{
    ProfThreadData * tdata = tsd.prof_tdata;
    if (create)
    {
        JE_ASSERT(tsd.reentrancyLevel() == 0);
        if (JE_UNLIKELY(tdata == nullptr))
        {
            if (tsd.nominal())
            {
                tdata = profTdataInit(tsd);
                tsd.prof_tdata = tdata;
            }
        }
        else if (JE_UNLIKELY(tdata->expired))
        {
            tdata = profTdataReinit(tsd, tdata);
            tsd.prof_tdata = tdata;
        }
        JE_ASSERT(tdata == nullptr || tdata->attached);
    }
    if (tdata != nullptr)
        profThreadNameAssert(tdata);
    return tdata;
}

/// jemalloc: prof_thread_name_clear (`prof_data.h`)
JE_ALWAYS_INLINE void profThreadNameClear(ProfThreadData * tdata)
{
    tdata->thread_name[0] = '\0';
}

/// jemalloc: prof_thread_name_empty (`prof_data.h`)
JE_ALWAYS_INLINE bool profThreadNameEmpty(const ProfThreadData * tdata)
{
    profThreadNameAssert(tdata);
    return tdata->thread_name[0] == '\0';
}

/// --- prof.c --------------------------------------------------------------------------------------------------------

/// Dump a heap profile when the total virtual memory reaches a new high (`prof.gdump`).
/// jemalloc: prof_gdump
void profGdump(ThreadState * tsdn);
/// An interval-triggered dump. jemalloc: prof_idump
void profIdump(ThreadState * tsdn);
/// `prof.dump`; returns true on error. jemalloc: prof_mdump
bool profMdump(ThreadState & tsd, const char * filename);

/// jemalloc: prof_active_get, prof_active_set (returns the old value)
bool profActiveGet(ThreadState * tsdn);
bool profActiveSet(ThreadState * tsdn, bool active);
/// jemalloc: prof_thread_name_get, prof_thread_name_set (returns an errno value)
const char * profThreadNameGet(ThreadState & tsd);
int profThreadNameSet(ThreadState & tsd, const char * thread_name);
/// jemalloc: prof_thread_active_get, prof_thread_active_set (returns true on error)
bool profThreadActiveGet(ThreadState & tsd);
bool profThreadActiveSet(ThreadState & tsd, bool active);
/// jemalloc: prof_thread_active_init_get, prof_thread_active_init_set (returns the old value)
bool profThreadActiveInitGet(ThreadState * tsdn);
bool profThreadActiveInitSet(ThreadState * tsdn, bool active_init);
/// jemalloc: prof_gdump_get, prof_gdump_set (returns the old value)
bool profGdumpGet(ThreadState * tsdn);
bool profGdumpSet(ThreadState * tsdn, bool gdump);

/// --- prof_data.c ---------------------------------------------------------------------------------------------------

/// Returns true on error. jemalloc: prof_data_init
bool profDataInit(ThreadState & tsd);
/// Track / untrack a live sampled allocation on its gctx's `frag_objs` list (ClickHouse fork).
/// jemalloc: prof_frag_track, prof_frag_untrack
void profFragTrack(ThreadState & tsd, Extent * edata, ProfThreadContext * tctx);
/// `profFragUntrack` is declared in Arena.h.
/// jemalloc: prof_lookup
ProfThreadContext * profLookup(ThreadState & tsd, ProfBacktrace * bt);
/// jemalloc: prof_thread_name_set_impl
int profThreadNameSetImpl(ThreadState & tsd, const char * thread_name);
/// jemalloc: prof_unbias_map_init
void profUnbiasMapInit();
/// Requires `prof_dump_mtx`. jemalloc: prof_dump_impl
void profDumpImpl(ThreadState & tsd, WriteCallback * prof_dump_write, void * cbopaque, ProfThreadData * tdata, bool leakcheck);
/// jemalloc: prof_bt_hash, prof_bt_keycomp
void profBtHash(const void * key, size_t r_hash[2]);
bool profBtKeycomp(const void * k1, const void * k2);
/// jemalloc: prof_tdata_init_impl
ProfThreadData *
profTdataInitImpl(ThreadState & tsd, uint64_t thr_uid, uint64_t thr_discrim, const char * thread_name, bool active);
/// jemalloc: prof_tdata_detach
void profTdataDetach(ThreadState & tsd, ProfThreadData * tdata);
/// `prof.reset`. jemalloc: prof_reset
void profReset(ThreadState & tsd, size_t lg_sample);
/// Requires `tctx->tdata->lock`, which is released. jemalloc: prof_tctx_try_destroy
void profTctxTryDestroy(ThreadState & tsd, ProfThreadContext * tctx);

/// Internal allocations of the profiler (`is_internal`, no tcache): `arena_get(TSDN_NULL, 0, true)` (tdata, gctx),
/// `arena_ichoose(tsd, NULL)` (tctx), `arena_get(tsdn, 0, false)` (recent records); `idalloctm(..., true, true)`.
void * profAllocArena0(ThreadState & tsd, size_t size, bool init_if_missing);
void * profAllocIchoose(ThreadState & tsd, size_t size);
void profIdalloc(ThreadState * tsdn, void * ptr);

/// The number of tdatas / backtraces (tests). jemalloc: prof_tdata_count, prof_bt_count
size_t profTdataCount();
size_t profBtCount();

/// --- prof_sys.c ----------------------------------------------------------------------------------------------------

/// jemalloc: bt_init
void btInit(ProfBacktrace * bt, void ** vec);
/// Calls the backtrace hook inside a reentrancy section. jemalloc: prof_backtrace
void profBacktrace(ThreadState & tsd, ProfBacktrace * bt);
/// The default backtrace hook (`unw_backtrace`). jemalloc: prof_backtrace_impl
void profBacktraceImpl(void ** vec, unsigned * len, unsigned max_len);
/// jemalloc: prof_hooks_init, prof_unwind_init
void profHooksInit();
void profUnwindInit();
/// jemalloc: prof_sys_thread_name_fetch
void profSysThreadNameFetch(ThreadState & tsd);
/// jemalloc: prof_getpid
int profGetpid();
/// Under `ctl_mtx`; returns true on error. jemalloc: prof_prefix_set
bool profPrefixSet(ThreadState * tsdn, const char * prefix);
/// jemalloc: prof_fdump_impl, prof_idump_impl, prof_mdump_impl, prof_gdump_impl
void profFdumpImpl(ThreadState & tsd);
void profIdumpImpl(ThreadState & tsd);
bool profMdumpImpl(ThreadState & tsd, const char * filename);
void profGdumpImpl(ThreadState & tsd);

/// --- prof_recent.c -------------------------------------------------------------------------------------------------

/// Requires `tctx->tdata->lock`. jemalloc: prof_recent_alloc_prepare
bool profRecentAllocPrepare(ThreadState & tsd, ProfThreadContext * tctx);
/// jemalloc: prof_recent_alloc
void profRecentAlloc(ThreadState & tsd, Extent * edata, size_t size, size_t usize);
/// `profRecentAllocReset` is declared in Arena.h.
/// jemalloc: prof_recent_alloc_max_ctl_read, prof_recent_alloc_max_ctl_write (returns the old value)
ssize_t profRecentAllocMaxCtlRead();
ssize_t profRecentAllocMaxCtlWrite(ThreadState & tsd, ssize_t max);
/// jemalloc: prof_recent_alloc_dump
void profRecentAllocDump(ThreadState & tsd, WriteCallback * write_cb, void * cbopaque);
/// Returns true on error. jemalloc: prof_recent_init
bool profRecentInit();

/// --- prof_stats.c --------------------------------------------------------------------------------------------------

/// jemalloc: prof_stats_inc, prof_stats_dec, prof_stats_get_live, prof_stats_get_accum
void profStatsInc(ThreadState & tsd, szind_t ind, size_t size);
void profStatsDec(ThreadState & tsd, szind_t ind, size_t size);
void profStatsGetLive(ThreadState & tsd, szind_t ind, ProfStats * stats);
void profStatsGetAccum(ThreadState & tsd, szind_t ind, ProfStats * stats);

}
