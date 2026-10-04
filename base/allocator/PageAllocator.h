#pragma once

/// The page allocator: acquires pages of memory for allocations.
/// jemalloc: `pac.h`, `src/pac.c` (`PageAllocator` = `pac_t`, "page allocator classic"), `pa.h`, `src/pa.c`,
/// `src/pa_extra.c` (`PaShard` = `pa_shard_t`), and the PAC part of `pai.h`.
///
/// HPA and SEC are dropped: the shard always uses the PAC (`use_hpa` / `ever_used_hpa` are always false), and the
/// page allocator interface (`pai_t` vtable) is devirtualized. The HPA-only parts of the observable state keep their
/// space in the structures (so that `sizeof(Arena)` matches `arena_t`), and the HPA-only operations are no-ops.

#include <allocator/Base.h>
#include <allocator/Common.h>
#include <allocator/Decay.h>
#include <allocator/ExpGrow.h>
#include <allocator/Extent.h>
#include <allocator/ExtentCache.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/ExtentPool.h>
#include <allocator/Mutex.h>
#include <allocator/NsTime.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>

#include <atomic>
#include <cstdint>
#include <sys/types.h>

namespace jemalloc
{

class ThreadState;

/// --- lockedint.h (with `JEMALLOC_ATOMIC_U64`, which every supported platform has) ----------------------------------

/// A 64-bit counter updated with relaxed atomics; the "associated mutex" of jemalloc's API is always null.
/// NOTE: a generic utility (also needed by the arena stats); it may move to its own header.
/// jemalloc: locked_u64_t
struct LockedU64
{
    std::atomic<uint64_t> val{0};

    /// jemalloc: locked_read_u64
    JE_ALWAYS_INLINE uint64_t read() const { return val.load(std::memory_order_relaxed); }

    /// jemalloc: locked_inc_u64
    JE_ALWAYS_INLINE void inc(uint64_t x) { val.fetch_add(x, std::memory_order_relaxed); }

    /// jemalloc: locked_dec_u64
    JE_ALWAYS_INLINE void dec(uint64_t x)
    {
        [[maybe_unused]] uint64_t r = val.fetch_sub(x, std::memory_order_relaxed);
        JE_ASSERT(r - x <= r);
    }

    /// Non-atomically sets `*this += src` (needs external synchronization; avoids the cost of a fetch-add).
    /// jemalloc: locked_inc_u64_unsynchronized
    JE_ALWAYS_INLINE void incUnsynchronized(uint64_t src)
    {
        uint64_t cur_dst = val.load(std::memory_order_relaxed);
        val.store(src + cur_dst, std::memory_order_relaxed);
    }

    /// jemalloc: locked_read_u64_unsynchronized
    JE_ALWAYS_INLINE uint64_t readUnsynchronized() const { return val.load(std::memory_order_relaxed); }

    /// jemalloc: locked_init_u64_unsynchronized
    JE_ALWAYS_INLINE void initUnsynchronized(uint64_t x) { val.store(x, std::memory_order_relaxed); }
};

static_assert(sizeof(LockedU64) == 8);

/// --- pac.h ---------------------------------------------------------------------------------------------------------

/// How "eager" decay/purging should be.
/// jemalloc: pac_purge_eagerness_t
enum PacPurgeEagerness : unsigned
{
    PAC_PURGE_ALWAYS,
    PAC_PURGE_NEVER,
    PAC_PURGE_ON_EPOCH_ADVANCE,
};

/// jemalloc: pac_decay_stats_t
struct DecayStats
{
    /// Total number of purge sweeps.
    LockedU64 npurge;
    /// Total number of madvise calls made.
    LockedU64 nmadvise;
    /// Total number of pages purged.
    LockedU64 purged;
};

/// Stats for a given index in the range [0, SC_NPSIZES] in the various extent caches. We track both bytes and the
/// number of extents: two extents in the same bucket may have different sizes if adjacent size classes differ by more
/// than a page, so bytes cannot always be derived from the number of extents.
/// jemalloc: pac_estats_t
struct PacExtentStats
{
    size_t ndirty;
    size_t dirty_bytes;
    size_t nmuzzy;
    size_t muzzy_bytes;
    size_t nretained;
    size_t retained_bytes;
};

/// jemalloc: pac_stats_t
struct PacStats
{
    DecayStats decay_dirty;
    DecayStats decay_muzzy;

    /// Number of unused virtual memory bytes currently retained. Retained bytes are technically mapped (though always
    /// decommitted or purged), but they are excluded from the mapped statistic. Derived.
    size_t retained = 0;

    /// Number of bytes currently mapped, excluding retained memory (and any base-allocated memory, which is tracked by
    /// the arena stats). Named "pac_mapped" to avoid confusion with the arena stats "mapped".
    std::atomic<size_t> pac_mapped{0};

    /// VM space had to be leaked (undocumented). Normally 0.
    std::atomic<size_t> abandoned_vm{0};
};

static_assert(sizeof(PacStats) == 72);

/// Page allocator classic: an implementation of the page allocator interface that can be used for arenas with custom
/// extent hooks, can always satisfy any allocation request (including highly-fragmentary ones), and can use efficient
/// OS-level zeroing primitives for demand-filled pages.
/// jemalloc: pac_t
class PageAllocator
{
public:
    constexpr PageAllocator() = default;

    PageAllocator(const PageAllocator &) = delete;
    PageAllocator & operator=(const PageAllocator &) = delete;

    /// Returns true on error.
    /// jemalloc: pac_init
    bool init(
        ThreadState * tsdn,
        Base * base_,
        ExtentMap * emap_,
        ExtentPool * edata_cache_,
        const NsTime & cur_time,
        size_t pac_oversize_threshold,
        ssize_t dirty_decay_ms,
        ssize_t muzzy_decay_ms,
        PacStats * pac_stats,
        Mutex * stats_mtx_);

    /// jemalloc: pac_mapped
    JE_ALWAYS_INLINE size_t mapped() const { return stats->pac_mapped.load(std::memory_order_relaxed); }

    /// jemalloc: pac_ehooks_get
    JE_ALWAYS_INLINE ExtentHooks * ehooksGet() const { return base->ehooksGet(); }

    /// --- The page allocator interface (jemalloc: `pai_t`, implemented by `pac_*_impl`) ------------------------------

    /// Returns null on failure.
    /// jemalloc: pac_alloc_impl (pai_alloc)
    Extent * alloc(ThreadState * tsdn, size_t size, size_t alignment, bool zero, bool guarded, bool frequent_reuse, bool * deferred_work_generated);

    /// Returns true on error.
    /// jemalloc: pac_expand_impl (pai_expand)
    bool expand(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, bool zero, bool * deferred_work_generated);

    /// Returns true on error.
    /// jemalloc: pac_shrink_impl (pai_shrink)
    bool shrink(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, bool * deferred_work_generated);

    /// jemalloc: pac_dalloc_impl (pai_dalloc)
    void dalloc(ThreadState * tsdn, Extent * edata, bool * deferred_work_generated);

    /// jemalloc: pac_time_until_deferred_work (pai_time_until_deferred_work)
    uint64_t timeUntilDeferredWork(ThreadState * tsdn);

    /// --- Purging (all purging functions require holding `decay->mtx`) ----------------------------------------------

    /// Decays the number of pages currently in the cache. This might not leave the cache empty if other threads are
    /// inserting dirty objects into it concurrently with the call.
    /// jemalloc: pac_decay_all
    void decayAll(ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, bool fully_decay);

    /// Updates decay settings for the current time, and conditionally purges in response (depending on `eagerness`).
    /// Returns whether or not the epoch advanced.
    /// jemalloc: pac_maybe_decay_purge
    bool maybeDecayPurge(ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, PacPurgeEagerness eagerness);

    /// Decay at most `npages_decay_max` pages without violating the invariant `ecache->npagesGet() >= npages_limit`.
    /// We need an upper bound on the number of pages in order to prevent unbounded growth (namely in stashed),
    /// otherwise unbounded new pages could be added to extents during the current decay run, so that the purging
    /// thread never finishes. Drops `decay->mtx` while purging.
    /// jemalloc: pac_decay_to_limit
    void decayToLimit(
        ThreadState * tsdn,
        Decay * decay,
        DecayStats * decay_stats,
        ExtentCache * ecache,
        bool fully_decay,
        size_t npages_limit,
        size_t npages_decay_max);

    /// Evicts extents from the cache into `result` until at least `npages_decay_max` pages are stashed (or the cache
    /// is down to `npages_limit`). Returns the number of stashed pages.
    /// jemalloc: pac_stash_decayed
    size_t stashDecayed(ThreadState * tsdn, ExtentCache * ecache, size_t npages_limit, size_t npages_decay_max, ExtentListInactive * result);

    /// Purges the stashed extents (dirty -> muzzy, or to retained). Returns the number of purged pages.
    /// jemalloc: pac_decay_stashed
    size_t decayStashed(
        ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, bool fully_decay, ExtentListInactive * decay_extents);

    /// Gets / sets the maximum amount that we'll grow an arena down the grow-retained pathways (unless forced to by an
    /// allocation request). `new_limit` is null for a query; `old_limit` is null if the previous value is not needed.
    /// Returns true on error (if the new limit is not valid).
    /// jemalloc: pac_retain_grow_limit_get_set
    bool retainGrowLimitGetSet(ThreadState * tsdn, size_t * old_limit, size_t * new_limit);

    /// Returns true on error.
    /// jemalloc: pac_decay_ms_set
    bool decayMsSet(ThreadState * tsdn, ExtentState state, ssize_t decay_ms, PacPurgeEagerness eagerness);

    /// jemalloc: pac_decay_ms_get
    ssize_t decayMsGet(ExtentState state);

    /// A no-op for now; purging is still done at the arena level.
    /// jemalloc: pac_reset
    void reset(ThreadState * tsdn);

    /// Destroys all retained extents (after everything has been decayed to retained).
    /// jemalloc: pac_destroy
    void destroy(ThreadState * tsdn);

    /// jemalloc: pac_decay_data_get
    void decayDataGet(ExtentState state, Decay ** r_decay, DecayStats ** r_decay_stats, ExtentCache ** r_ecache);

    /// The `pai_t` vtable of jemalloc (must be the first member). The interface is devirtualized; the space is kept so
    /// that the layout (and `sizeof(arena_t)`, observable through `stats.metadata`) is identical.
    void * pai_placeholder[5] = {};

    /// Collections of extents that were previously allocated. These are used when allocating extents, in an attempt
    /// to re-use address space. Synchronization: internal.
    ExtentCache ecache_dirty;
    ExtentCache ecache_muzzy;
    ExtentCache ecache_retained;

    Base * base = nullptr;
    ExtentMap * emap = nullptr;
    ExtentPool * edata_cache = nullptr;

    /// The grow info for the retained cache.
    ExpGrow exp_grow{};
    /// "extent_grow", `MutexRank::EXTENT_GROW`.
    Mutex grow_mtx;

    /// Special allocator for guarded frequently reused extents.
    SanBumpAlloc sba;

    /// How large extents should be before getting auto-purged.
    std::atomic<size_t> oversize_threshold{0};

    /// Decay-based purging state, responsible for scheduling extent state transitions. Synchronization: via the
    /// internal mutex.
    Decay decay_dirty; /// dirty --> muzzy
    Decay decay_muzzy; /// muzzy --> retained

    /// Always null (`JEMALLOC_ATOMIC_U64`).
    Mutex * stats_mtx = nullptr;
    PacStats * stats = nullptr;

    /// Extent serial number generator state.
    std::atomic<size_t> extent_sn_next{0};

private:
    /// jemalloc: pac_may_have_muzzy
    JE_ALWAYS_INLINE bool mayHaveMuzzy() { return decayMsGet(extent_state_muzzy) != 0; }

    /// jemalloc: pac_alloc_real
    Extent * allocReal(ThreadState * tsdn, ExtentHooks * ehooks, size_t size, size_t alignment, bool zero, bool guarded);

    /// jemalloc: pac_alloc_new_guarded
    Extent * allocNewGuarded(ThreadState * tsdn, ExtentHooks * ehooks, size_t size, size_t alignment, bool zero, bool frequent_reuse);

    /// jemalloc: pac_decay_try_purge
    void decayTryPurge(ThreadState * tsdn, Decay * decay, DecayStats * decay_stats, ExtentCache * ecache, size_t current_npages, size_t npages_limit);
};

/// The size of the over-allocated chunk taken from the retained cache (the rest goes to the dirty cache): the size
/// rounded up to the classic jemalloc size class, but not beyond the next huge page boundary.
/// jemalloc: pac_alloc_retained_batched_size
size_t pacAllocRetainedBatchedSize(size_t size);

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(PageAllocator) == (LG_PAGE == 12 ? 62280 : (LG_PAGE == 14 ? 59928 : 57624)), "pac_t size (aarch64 glibc)");
#endif

/// --- pa.h ----------------------------------------------------------------------------------------------------------

/// The stats for a particular shard. Because of the way the ctl module handles stats epoch data collection (it has its
/// own arena stats, and merges the stats from each arena into it), this lives in the arena stats; the shard has a
/// pointer. Derived fields are not maintained on their own; their values are derived during stats merges.
/// jemalloc: pa_shard_stats_t
struct PaShardStats
{
    /// Number of `Extent` structs allocated by base, but not being used. Derived.
    size_t edata_avail = 0;
    /// Stats specific to the PAC.
    PacStats pac_stats;
};

static_assert(sizeof(PaShardStats) == 80);

/// The size of `hpa_shard_t` (HPA is dropped; the space is kept for an identical `sizeof(arena_t)`).
inline constexpr size_t HPA_SHARD_PLACEHOLDER_SIZE = 5888;

/// The local allocator handle. Keeps the state necessary to satisfy page-sized allocations.
///
/// The contents are mostly internal to the PA module. The key exception is that arena decay code is allowed to grab
/// pointers to the dirty and muzzy caches and decays for a couple of queries, passing them back to a PA function, or
/// acquiring `decay.mtx` and looking at `decay.purging`: PA decides what and how to purge, the arena code decides when
/// and where (e.g. on what thread).
/// jemalloc: pa_shard_t
class PaShard
{
public:
    constexpr PaShard() = default;

    PaShard(const PaShard &) = delete;
    PaShard & operator=(const PaShard &) = delete;

    /// Returns true on error. Zeroes `*stats_`.
    /// jemalloc: pa_shard_init (`central` is the HPA central, dropped)
    bool init(
        ThreadState * tsdn,
        ExtentMap * emap_,
        Base * base_,
        unsigned ind_,
        PaShardStats * stats_,
        Mutex * stats_mtx_,
        const NsTime & cur_time,
        size_t pac_oversize_threshold,
        ssize_t dirty_decay_ms,
        ssize_t muzzy_decay_ms);

    /// HPA is dropped: always fails (returns true). It is never called, because `opt.hpa` is forced to false at boot.
    /// jemalloc: pa_shard_enable_hpa
    bool enableHpa(ThreadState * tsdn);

    /// jemalloc: pa_shard_disable_hpa
    void disableHpa(ThreadState * tsdn);

    /// The PA-specific parts of arena reset (i.e. freeing all active allocations).
    /// jemalloc: pa_shard_reset
    void reset(ThreadState * tsdn);

    /// Destroy all the remaining retained extents. Should only be called after decaying all active, dirty, and muzzy
    /// extents to the retained state, as the last step in destroying the shard.
    /// jemalloc: pa_shard_destroy
    void destroy(ThreadState * tsdn);

    /// Flush any caches used by the shard (HPA only: a no-op).
    /// jemalloc: pa_shard_flush
    void flush(ThreadState * tsdn);

    /// Gets an extent for the given allocation.
    /// jemalloc: pa_alloc
    Extent * alloc(
        ThreadState * tsdn, size_t size, size_t alignment, bool slab, szind_t szind, bool zero, bool guarded, bool * deferred_work_generated);

    /// Returns true on error, in which case nothing changed.
    /// jemalloc: pa_expand
    bool expand(
        ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, szind_t szind, bool zero, bool * deferred_work_generated);

    /// The same. Sets `*deferred_work_generated` if new dirty pages were produced.
    /// jemalloc: pa_shrink
    bool shrink(ThreadState * tsdn, Extent * edata, size_t old_size, size_t new_size, szind_t szind, bool * deferred_work_generated);

    /// Frees the given extent back to the page allocator. Sets `*deferred_work_generated` if new dirty pages were
    /// produced (always for now).
    /// jemalloc: pa_dalloc
    void dalloc(ThreadState * tsdn, Extent * edata, bool * deferred_work_generated);

    /// jemalloc: pa_decay_ms_set
    bool decayMsSet(ThreadState * tsdn, ExtentState state, ssize_t decay_ms, PacPurgeEagerness eagerness);

    /// jemalloc: pa_decay_ms_get
    ssize_t decayMsGet(ExtentState state);

    /// HPA only: a no-op.
    /// jemalloc: pa_shard_set_deferral_allowed
    void setDeferralAllowed(ThreadState * tsdn, bool deferral_allowed);

    /// HPA only: a no-op.
    /// jemalloc: pa_shard_do_deferred_work
    void doDeferredWork(ThreadState * tsdn);

    /// The time until the next deferred work ought to happen (the soonest of all deferred things).
    /// jemalloc: pa_shard_time_until_deferred_work
    uint64_t timeUntilDeferredWork(ThreadState * tsdn);

    /// jemalloc: pa_shard_dont_decay_muzzy
    JE_ALWAYS_INLINE bool dontDecayMuzzy()
    {
        return pac.ecache_muzzy.npagesGet() == 0 && pac.decayMsGet(extent_state_muzzy) <= 0;
    }

    /// jemalloc: pa_shard_ehooks_get
    JE_ALWAYS_INLINE ExtentHooks * ehooksGet() const { return base->ehooksGet(); }

    /// --- pa_extra.c ------------------------------------------------------------------------------------------------

    /// The fork phases are synchronized with the arena fork phase numbering (that's why there's no prefork1).
    /// jemalloc: pa_shard_prefork0
    void prefork0(ThreadState * tsdn);
    /// jemalloc: pa_shard_prefork2
    void prefork2(ThreadState * tsdn);
    /// jemalloc: pa_shard_prefork3
    void prefork3(ThreadState * tsdn);
    /// jemalloc: pa_shard_prefork4
    void prefork4(ThreadState * tsdn);
    /// jemalloc: pa_shard_prefork5
    void prefork5(ThreadState * tsdn);
    /// jemalloc: pa_shard_postfork_parent
    void postforkParent(ThreadState * tsdn);
    /// jemalloc: pa_shard_postfork_child
    void postforkChild(ThreadState * tsdn);

    /// jemalloc: pa_shard_nactive
    JE_ALWAYS_INLINE size_t nactiveGet() const { return nactive.load(std::memory_order_relaxed); }

    /// jemalloc: pa_shard_ndirty
    JE_ALWAYS_INLINE size_t ndirtyGet() const { return pac.ecache_dirty.npagesGet(); }

    /// jemalloc: pa_shard_nmuzzy
    JE_ALWAYS_INLINE size_t nmuzzyGet() const { return pac.ecache_muzzy.npagesGet(); }

    /// jemalloc: pa_shard_basic_stats_merge
    void basicStatsMerge(size_t * nactive_, size_t * ndirty, size_t * nmuzzy) const;

    /// Adds the derived and counter stats into `pa_shard_stats_out`, assigns the per-size extent stats
    /// (`estats_out[0 .. SC_NPSIZES)`), adds the resident bytes. The HPA stats (`hpa_shard_stats_t`) are not touched
    /// (they are only merged if the HPA was ever used, which never happens).
    /// jemalloc: pa_shard_stats_merge
    void statsMerge(ThreadState * tsdn, PaShardStats * pa_shard_stats_out, PacExtentStats * estats_out, size_t * resident);

    /// Reads the PA-owned mutex stats into the output array (indexed by `MutexProfArenaInd`). The HPA/SEC entries are
    /// left untouched.
    /// jemalloc: pa_shard_mtx_stats_read
    void mtxStatsRead(ThreadState * tsdn, MutexProfData (&mutex_prof_data)[mutex_prof_num_arena_mutexes]);

    /// The central PA this shard is associated with (`pa_central_t *`: HPA only, dropped; always null).
    void * central = nullptr;

    /// Number of pages in active extents. Synchronization: atomic.
    std::atomic<size_t> nactive{0};

    /// Whether or not we should prefer the hugepage allocator (always false: HPA is dropped).
    std::atomic<bool> use_hpa{false};

    /// If we never used the HPA to begin with, it wasn't initialized (always false).
    bool ever_used_hpa = false;

    /// Allocates from a PAC.
    PageAllocator pac;

    /// `hpa_shard_t hpa_shard` (dropped; the space is kept for an identical layout).
    alignas(64) std::byte hpa_shard_placeholder[HPA_SHARD_PLACEHOLDER_SIZE] = {};

    /// The source of `Extent` objects.
    ExtentPool edata_cache;

    unsigned ind = 0;

    /// Always null (`JEMALLOC_ATOMIC_U64`).
    Mutex * stats_mtx = nullptr;
    PaShardStats * stats = nullptr;

    /// The emap this shard is tied to.
    ExtentMap * emap = nullptr;

    /// The base from which we get the extent hooks and allocate metadata.
    Base * base = nullptr;

private:
    /// jemalloc: pa_nactive_add
    JE_ALWAYS_INLINE void nactiveAdd(size_t add_pages) { nactive.fetch_add(add_pages, std::memory_order_relaxed); }

    /// jemalloc: pa_nactive_sub
    JE_ALWAYS_INLINE void nactiveSub(size_t sub_pages)
    {
        JE_ASSERT(nactiveGet() >= sub_pages);
        nactive.fetch_sub(sub_pages, std::memory_order_relaxed);
    }
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(PaShard) == (LG_PAGE == 12 ? 68416 : (LG_PAGE == 14 ? 66048 : 63744)), "pa_shard_t size (aarch64 glibc)");
static_assert(alignof(PaShard) == 64);
#endif

}
