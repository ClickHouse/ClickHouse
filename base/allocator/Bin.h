#pragma once

/// A bin: the set of slabs currently used for allocations of one small size class (one shard of it) in an arena.
/// jemalloc: `bin.h`, `src/bin.c`, `bin_inlines.h`, `bin_stats.h`, `bin_types.h` (`Bin` = `bin_t`).
///
/// `bin_shard_sizes_boot` / `bin_update_shard_size` and `BIN_SHARDS_MAX` live in SizeClasses.h (bin_info); the
/// per-thread shard binding (`tsd_binshards_t`) is `ThreadStateBinShards` in ThreadState.h.

#include <allocator/Bitmap.h>
#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/Mutex.h>
#include <allocator/SizeClasses.h>

#include <cstdint>

namespace jemalloc
{

class Arena;
class ThreadState;

/// jemalloc: bin_stats_t
struct BinStats
{
    /// Total number of allocation/deallocation requests served directly by the bin. Note that tcache may allocate an
    /// object, then recycle it many times, resulting many increments to nrequests, but only one each to nmalloc and
    /// ndalloc.
    uint64_t num_allocations = 0;
    uint64_t num_deallocations = 0;
    /// Number of allocation requests that correspond to the size of this bin. This includes requests served by
    /// tcache, though tcache only periodically merges into this counter.
    uint64_t num_requests = 0;
    /// Current number of regions of this size class, including regions currently cached by tcache.
    size_t current_regions = 0;
    /// Number of tcache fills from this bin.
    uint64_t num_fills = 0;
    /// Number of tcache flushes to this bin.
    uint64_t num_flushes = 0;
    /// Total number of slabs created for this bin's size class.
    uint64_t num_slabs = 0;
    /// Total number of slabs reused by extracting them from the slabs heap for this bin's size class.
    uint64_t slab_changes = 0;
    /// Current number of slabs in this bin.
    size_t current_slabs = 0;
    /// Current size of nonfull slabs heap in this bin.
    size_t non_full_slabs = 0;
};

static_assert(sizeof(BinStats) == 80);

/// jemalloc: bin_stats_data_t
struct BinStatsData
{
    BinStats stats_data;
    MutexProfilingData mutex_data;
};

/// `arena_bin_idx_division_info[bin_idx]` divides by the region size of the bin (defined in Arena.cpp, set by `arenaBoot`).
/// jemalloc: arena_binind_div_info
extern constinit DivisionInfo arena_bin_idx_division_info[SIZE_CLASS_NUM_BINS];

/// The information that the common paths need during tcache flushes. By force-inlining these paths, and using local
/// copies of data (so that the compiler knows it's constant), we avoid a whole bunch of redundant loads and stores by
/// leaving this information in registers.
/// jemalloc: bin_dalloc_locked_info_t
struct BinDeallocateLockedInfo
{
    DivisionInfo division_info;
    uint32_t num_regions;
    uint64_t num_deallocations;
};

/// All operations on the fields require holding `lock`.
/// jemalloc: bin_t
class Bin
{
public:
    constexpr Bin() = default;

    Bin(const Bin &) = delete;
    Bin & operator=(const Bin &) = delete;

    /// Initializes a bin to empty. Returns true on error.
    /// jemalloc: bin_init
    bool init();

    /// jemalloc: bin_prefork
    void prefork(ThreadState * thread_state) { lock.prefork(thread_state); }
    /// jemalloc: bin_postfork_parent
    void postforkParent(ThreadState * thread_state) { lock.postforkParent(thread_state); }
    /// jemalloc: bin_postfork_child
    void postforkChild(ThreadState * thread_state) { lock.postforkChild(thread_state); }

    /// --- Slab region allocation (no bin state involved) ------------------------------------------------------------

    /// jemalloc: bin_slab_reg_alloc
    static void * slabRegionAlloc(Extent * slab, const BinInfo & bin_info);

    /// jemalloc: bin_slab_reg_alloc_batch
    static void slabRegionAllocBatch(Extent * slab, const BinInfo & bin_info, unsigned count, void ** ptrs);

    /// --- Slab list management --------------------------------------------------------------------------------------

    /// jemalloc: bin_slabs_nonfull_insert
    void slabsNonFullInsert(Extent * slab);
    /// jemalloc: bin_slabs_nonfull_remove
    void slabsNonFullRemove(Extent * slab);
    /// jemalloc: bin_slabs_nonfull_tryget
    Extent * slabsNonFullTryGet();
    /// Tracking extents is required by arena reset, which is not allowed for auto arenas. Bypass this step to avoid
    /// touching the extent linkage (often results in cache misses) for auto arenas.
    /// jemalloc: bin_slabs_full_insert
    void slabsFullInsert(bool is_auto, Extent * slab);
    /// jemalloc: bin_slabs_full_remove
    void slabsFullRemove(bool is_auto, Extent * slab);

    /// --- Slab association / demotion -------------------------------------------------------------------------------

    /// jemalloc: bin_dissociate_slab
    void dissociateSlab(bool is_auto, Extent * slab);

    /// Make sure that if `current_slab` is non-null, it refers to the oldest/lowest non-full slab. It is okay to null
    /// `current_slab` out rather than proactively keeping it pointing at the oldest/lowest non-full slab.
    /// jemalloc: bin_lower_slab
    void lowerSlab(ThreadState * thread_state, bool is_auto, Extent * slab);

    /// --- Deallocation helpers (called under the bin lock) ----------------------------------------------------------

    /// jemalloc: bin_dalloc_slab_prepare
    void deallocateSlabPrepare(ThreadState * thread_state, Extent * slab);
    /// jemalloc: bin_dalloc_locked_handle_newly_empty
    void deallocateLockedHandleNewlyEmpty(ThreadState * thread_state, bool is_auto, Extent * slab);
    /// jemalloc: bin_dalloc_locked_handle_newly_nonempty
    void deallocateLockedHandleNewlyNonempty(ThreadState * thread_state, bool is_auto, Extent * slab);

    /// --- Slabcur refill and allocation -----------------------------------------------------------------------------

    /// jemalloc: bin_refill_slabcur_with_fresh_slab
    void refillCurrentSlabWithFreshSlab(ThreadState * thread_state, SizeClassIdx bin_idx, Extent * fresh_slab);
    /// jemalloc: bin_malloc_with_fresh_slab
    void * mallocWithFreshSlab(ThreadState * thread_state, SizeClassIdx bin_idx, Extent * fresh_slab);
    /// Returns true if no usable slab was found (`current_slab` is null then).
    /// jemalloc: bin_refill_slabcur_no_fresh_slab
    bool refillCurrentSlabNoFreshSlab(ThreadState * thread_state, bool is_auto);
    /// jemalloc: bin_malloc_no_fresh_slab
    void * mallocNoFreshSlab(ThreadState * thread_state, bool is_auto, SizeClassIdx bin_idx);

    /// --- Locked deallocation (bin_inlines.h) -----------------------------------------------------------------------

    /// Find the region index of a pointer within a slab.
    /// jemalloc: bin_slab_regind_impl
    static ALLOCATOR_ALWAYS_INLINE size_t
    slabRegionIdxImpl(const DivisionInfo & division_info, SizeClassIdx bin_idx, const Extent * slab, const void * ptr)
    {
        /// Freeing a pointer outside the slab can cause assertion failure.
        ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(ptr) >= reinterpret_cast<uintptr_t>(slab->addr()));
        ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(slab->past()));
        /// Freeing an interior pointer can cause assertion failure.
        ALLOCATOR_ASSERT(
            (reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(slab->addr())) % bin_infos[bin_idx].region_size == 0);

        size_t diff = size_t(reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(slab->addr()));

        /// Avoid doing division with a variable divisor.
        size_t region_idx = division_info.compute(diff);
        ALLOCATOR_ASSERT(region_idx < bin_infos[bin_idx].num_regions);
        return region_idx;
    }

    /// jemalloc: bin_slab_regind
    static ALLOCATOR_ALWAYS_INLINE size_t
    slabRegionIdx(const BinDeallocateLockedInfo & info, SizeClassIdx bin_idx, const Extent * slab, const void * ptr)
    {
        return slabRegionIdxImpl(info.division_info, bin_idx, slab, ptr);
    }

    /// jemalloc: bin_dalloc_locked_begin
    static ALLOCATOR_ALWAYS_INLINE void deallocateLockedBegin(BinDeallocateLockedInfo & info, SizeClassIdx bin_idx)
    {
        info.division_info = arena_bin_idx_division_info[bin_idx];
        info.num_regions = bin_infos[bin_idx].num_regions;
        info.num_deallocations = 0;
    }

    /// Does the deallocation work associated with freeing a single pointer (a "step") in between a
    /// `deallocateLockedBegin` and `deallocateLockedFinish` call.
    ///
    /// Returns true if `Arena::slabDalloc` must be called on the slab. Doesn't do stats updates, which happen during
    /// finish (this lets running counts get left in a register).
    /// jemalloc: bin_dalloc_locked_step
    ALLOCATOR_ALWAYS_INLINE bool deallocateLockedStep(
        ThreadState * thread_state, bool is_auto, BinDeallocateLockedInfo & info, SizeClassIdx bin_idx, Extent * slab, void * ptr)
    {
        const BinInfo & bin_info = bin_infos[bin_idx];
        size_t region_idx = slabRegionIdx(info, bin_idx, slab, ptr);
        SlabData * slab_data = slab->slabData();

        ALLOCATOR_ASSERT(slab->numFree() < bin_info.num_regions);
        /// Freeing an unallocated pointer can cause assertion failure.
        ALLOCATOR_ASSERT(bitmapGet(slab_data->bitmap, bin_info.bitmap_info, region_idx));

        bitmapUnset(slab_data->bitmap, bin_info.bitmap_info, region_idx);
        slab->numFreeIncrement();

        if constexpr (config::stats)
            ++info.num_deallocations;

        unsigned num_free = slab->numFree();
        if (num_free == bin_info.num_regions)
        {
            deallocateLockedHandleNewlyEmpty(thread_state, is_auto, slab);
            return true;
        }
        else if (num_free == 1 && slab != current_slab)
        {
            deallocateLockedHandleNewlyNonempty(thread_state, is_auto, slab);
        }
        return false;
    }

    /// jemalloc: bin_dalloc_locked_finish
    ALLOCATOR_ALWAYS_INLINE void deallocateLockedFinish(ThreadState * /*tsdn*/, const BinDeallocateLockedInfo & info)
    {
        if constexpr (config::stats)
        {
            stats.num_deallocations += info.num_deallocations;
            ALLOCATOR_ASSERT(stats.current_regions >= size_t(info.num_deallocations));
            stats.current_regions -= size_t(info.num_deallocations);
        }
    }

    /// --- Stats -----------------------------------------------------------------------------------------------------

    /// jemalloc: bin_stats_merge
    void statsMerge(ThreadState * thread_state, BinStatsData & dst_bin_stats);

    /// --- Data (the layout is that of `bin_t`) ----------------------------------------------------------------------

    /// "bin", `MutexRank::BIN` (a leaf: nothing else may be acquired while holding it).
    Mutex lock;

    /// Bin statistics. These get touched every time the lock is acquired, so put them close by in the hopes of
    /// getting some cache locality.
    BinStats stats;

    /// Current slab being used to service allocations of this bin's size class. `current_slab` is independent of
    /// `slabs_non_full` / `slabs_full`; whenever `current_slab` is reassigned, the previous slab must be deallocated or
    /// inserted into `slabs_non_full` / `slabs_full`.
    Extent * current_slab = nullptr;

    /// Heap of non-full slabs. This heap is used to assure that new allocations come from the non-full slab that is
    /// oldest/lowest in memory.
    ExtentHeap slabs_non_full;

    /// List used to track full slabs (only for manual arenas).
    ExtentListActive slabs_full;
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Bin) == 232, "bin_t size (aarch64 glibc)");
#endif
static_assert(offsetof(Bin, stats) == sizeof(Mutex));

/// Bin selection: the thread's shard for `bin_idx` (shard 0 without tsd or before the thread is bound to an arena).
/// jemalloc: bin_choose
Bin * binChoose(ThreadState * thread_state, Arena * arena, SizeClassIdx bin_idx, unsigned * bin_shard_ptr);

}
