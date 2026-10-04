#pragma once

/// A bin: the set of slabs currently used for allocations of one small size class (one shard of it) in an arena.
/// jemalloc: `bin.h`, `src/bin.c`, `bin_inlines.h`, `bin_stats.h`, `bin_types.h` (`Bin` = `bin_t`).
///
/// `bin_shard_sizes_boot` / `bin_update_shard_size` and `BIN_SHARDS_MAX` live in SizeClasses.h (bin_info); the
/// per-thread shard binding (`tsd_binshards_t`) is `TsdBinshards` in ThreadState.h.

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
    uint64_t nmalloc = 0;
    uint64_t ndalloc = 0;
    /// Number of allocation requests that correspond to the size of this bin. This includes requests served by
    /// tcache, though tcache only periodically merges into this counter.
    uint64_t nrequests = 0;
    /// Current number of regions of this size class, including regions currently cached by tcache.
    size_t curregs = 0;
    /// Number of tcache fills from this bin.
    uint64_t nfills = 0;
    /// Number of tcache flushes to this bin.
    uint64_t nflushes = 0;
    /// Total number of slabs created for this bin's size class.
    uint64_t nslabs = 0;
    /// Total number of slabs reused by extracting them from the slabs heap for this bin's size class.
    uint64_t reslabs = 0;
    /// Current number of slabs in this bin.
    size_t curslabs = 0;
    /// Current size of nonfull slabs heap in this bin.
    size_t nonfull_slabs = 0;
};

static_assert(sizeof(BinStats) == 80);

/// jemalloc: bin_stats_data_t
struct BinStatsData
{
    BinStats stats_data;
    MutexProfData mutex_data;
};

/// `arena_binind_div_info[binind]` divides by the region size of the bin (defined in Arena.cpp, set by `arenaBoot`).
/// jemalloc: arena_binind_div_info
extern constinit DivInfo arena_binind_div_info[SC_NBINS];

/// The information that the common paths need during tcache flushes. By force-inlining these paths, and using local
/// copies of data (so that the compiler knows it's constant), we avoid a whole bunch of redundant loads and stores by
/// leaving this information in registers.
/// jemalloc: bin_dalloc_locked_info_t
struct BinDallocLockedInfo
{
    DivInfo div_info;
    uint32_t nregs;
    uint64_t ndalloc;
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
    void prefork(ThreadState * tsdn) { lock.prefork(tsdn); }
    /// jemalloc: bin_postfork_parent
    void postforkParent(ThreadState * tsdn) { lock.postforkParent(tsdn); }
    /// jemalloc: bin_postfork_child
    void postforkChild(ThreadState * tsdn) { lock.postforkChild(tsdn); }

    /// --- Slab region allocation (no bin state involved) ------------------------------------------------------------

    /// jemalloc: bin_slab_reg_alloc
    static void * slabRegAlloc(Extent * slab, const BinInfo & bin_info);

    /// jemalloc: bin_slab_reg_alloc_batch
    static void slabRegAllocBatch(Extent * slab, const BinInfo & bin_info, unsigned cnt, void ** ptrs);

    /// --- Slab list management --------------------------------------------------------------------------------------

    /// jemalloc: bin_slabs_nonfull_insert
    void slabsNonfullInsert(Extent * slab);
    /// jemalloc: bin_slabs_nonfull_remove
    void slabsNonfullRemove(Extent * slab);
    /// jemalloc: bin_slabs_nonfull_tryget
    Extent * slabsNonfullTryget();
    /// Tracking extents is required by arena reset, which is not allowed for auto arenas. Bypass this step to avoid
    /// touching the extent linkage (often results in cache misses) for auto arenas.
    /// jemalloc: bin_slabs_full_insert
    void slabsFullInsert(bool is_auto, Extent * slab);
    /// jemalloc: bin_slabs_full_remove
    void slabsFullRemove(bool is_auto, Extent * slab);

    /// --- Slab association / demotion -------------------------------------------------------------------------------

    /// jemalloc: bin_dissociate_slab
    void dissociateSlab(bool is_auto, Extent * slab);

    /// Make sure that if `slabcur` is non-null, it refers to the oldest/lowest non-full slab. It is okay to null
    /// `slabcur` out rather than proactively keeping it pointing at the oldest/lowest non-full slab.
    /// jemalloc: bin_lower_slab
    void lowerSlab(ThreadState * tsdn, bool is_auto, Extent * slab);

    /// --- Deallocation helpers (called under the bin lock) ----------------------------------------------------------

    /// jemalloc: bin_dalloc_slab_prepare
    void dallocSlabPrepare(ThreadState * tsdn, Extent * slab);
    /// jemalloc: bin_dalloc_locked_handle_newly_empty
    void dallocLockedHandleNewlyEmpty(ThreadState * tsdn, bool is_auto, Extent * slab);
    /// jemalloc: bin_dalloc_locked_handle_newly_nonempty
    void dallocLockedHandleNewlyNonempty(ThreadState * tsdn, bool is_auto, Extent * slab);

    /// --- Slabcur refill and allocation -----------------------------------------------------------------------------

    /// jemalloc: bin_refill_slabcur_with_fresh_slab
    void refillSlabcurWithFreshSlab(ThreadState * tsdn, szind_t binind, Extent * fresh_slab);
    /// jemalloc: bin_malloc_with_fresh_slab
    void * mallocWithFreshSlab(ThreadState * tsdn, szind_t binind, Extent * fresh_slab);
    /// Returns true if no usable slab was found (`slabcur` is null then).
    /// jemalloc: bin_refill_slabcur_no_fresh_slab
    bool refillSlabcurNoFreshSlab(ThreadState * tsdn, bool is_auto);
    /// jemalloc: bin_malloc_no_fresh_slab
    void * mallocNoFreshSlab(ThreadState * tsdn, bool is_auto, szind_t binind);

    /// --- Locked deallocation (bin_inlines.h) -----------------------------------------------------------------------

    /// Find the region index of a pointer within a slab.
    /// jemalloc: bin_slab_regind_impl
    static JE_ALWAYS_INLINE size_t slabRegindImpl(const DivInfo & div_info, szind_t binind, const Extent * slab, const void * ptr)
    {
        /// Freeing a pointer outside the slab can cause assertion failure.
        JE_ASSERT(reinterpret_cast<uintptr_t>(ptr) >= reinterpret_cast<uintptr_t>(slab->addr()));
        JE_ASSERT(reinterpret_cast<uintptr_t>(ptr) < reinterpret_cast<uintptr_t>(slab->past()));
        /// Freeing an interior pointer can cause assertion failure.
        JE_ASSERT((reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(slab->addr())) % bin_infos[binind].reg_size == 0);

        size_t diff = size_t(reinterpret_cast<uintptr_t>(ptr) - reinterpret_cast<uintptr_t>(slab->addr()));

        /// Avoid doing division with a variable divisor.
        size_t regind = div_info.compute(diff);
        JE_ASSERT(regind < bin_infos[binind].nregs);
        return regind;
    }

    /// jemalloc: bin_slab_regind
    static JE_ALWAYS_INLINE size_t slabRegind(const BinDallocLockedInfo & info, szind_t binind, const Extent * slab, const void * ptr)
    {
        return slabRegindImpl(info.div_info, binind, slab, ptr);
    }

    /// jemalloc: bin_dalloc_locked_begin
    static JE_ALWAYS_INLINE void dallocLockedBegin(BinDallocLockedInfo & info, szind_t binind)
    {
        info.div_info = arena_binind_div_info[binind];
        info.nregs = bin_infos[binind].nregs;
        info.ndalloc = 0;
    }

    /// Does the deallocation work associated with freeing a single pointer (a "step") in between a
    /// `dallocLockedBegin` and `dallocLockedFinish` call.
    ///
    /// Returns true if `Arena::slabDalloc` must be called on the slab. Doesn't do stats updates, which happen during
    /// finish (this lets running counts get left in a register).
    /// jemalloc: bin_dalloc_locked_step
    JE_ALWAYS_INLINE bool dallocLockedStep(
        ThreadState * tsdn, bool is_auto, BinDallocLockedInfo & info, szind_t binind, Extent * slab, void * ptr)
    {
        const BinInfo & bin_info = bin_infos[binind];
        size_t regind = slabRegind(info, binind, slab, ptr);
        SlabData * slab_data = slab->slabData();

        JE_ASSERT(slab->nfree() < bin_info.nregs);
        /// Freeing an unallocated pointer can cause assertion failure.
        JE_ASSERT(bitmapGet(slab_data->bitmap, bin_info.bitmap_info, regind));

        bitmapUnset(slab_data->bitmap, bin_info.bitmap_info, regind);
        slab->nfreeInc();

        if constexpr (config::stats)
            ++info.ndalloc;

        unsigned nfree = slab->nfree();
        if (nfree == bin_info.nregs)
        {
            dallocLockedHandleNewlyEmpty(tsdn, is_auto, slab);
            return true;
        }
        else if (nfree == 1 && slab != slabcur)
        {
            dallocLockedHandleNewlyNonempty(tsdn, is_auto, slab);
        }
        return false;
    }

    /// jemalloc: bin_dalloc_locked_finish
    JE_ALWAYS_INLINE void dallocLockedFinish(ThreadState * /*tsdn*/, const BinDallocLockedInfo & info)
    {
        if constexpr (config::stats)
        {
            stats.ndalloc += info.ndalloc;
            JE_ASSERT(stats.curregs >= size_t(info.ndalloc));
            stats.curregs -= size_t(info.ndalloc);
        }
    }

    /// --- Stats -----------------------------------------------------------------------------------------------------

    /// jemalloc: bin_stats_merge
    void statsMerge(ThreadState * tsdn, BinStatsData & dst_bin_stats);

    /// --- Data (the layout is that of `bin_t`) ----------------------------------------------------------------------

    /// "bin", `MutexRank::BIN` (a leaf: nothing else may be acquired while holding it).
    Mutex lock;

    /// Bin statistics. These get touched every time the lock is acquired, so put them close by in the hopes of
    /// getting some cache locality.
    BinStats stats;

    /// Current slab being used to service allocations of this bin's size class. `slabcur` is independent of
    /// `slabs_nonfull` / `slabs_full`; whenever `slabcur` is reassigned, the previous slab must be deallocated or
    /// inserted into `slabs_nonfull` / `slabs_full`.
    Extent * slabcur = nullptr;

    /// Heap of non-full slabs. This heap is used to assure that new allocations come from the non-full slab that is
    /// oldest/lowest in memory.
    ExtentHeap slabs_nonfull;

    /// List used to track full slabs (only for manual arenas).
    ExtentListActive slabs_full;
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Bin) == 232, "bin_t size (aarch64 glibc)");
#endif
static_assert(offsetof(Bin, stats) == sizeof(Mutex));

/// Bin selection: the thread's shard for `binind` (shard 0 without tsd or before the thread is bound to an arena).
/// jemalloc: bin_choose
Bin * binChoose(ThreadState * tsdn, Arena * arena, szind_t binind, unsigned * binshard_p);

}
