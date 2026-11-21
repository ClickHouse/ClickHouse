#pragma once

/// The metadata allocator: a bump allocator over mmapped blocks that never returns memory.
/// jemalloc: `base.h`, `src/base.c`.
///
/// One `Base` per arena (arena 0 uses `b0`). It hands out zeroed memory for arena structures, `Extent` structures,
/// rtree nodes and tcache stacks. Its size, the block size series, the bump order and the stats are observable
/// through `stats.metadata`, `stats.resident`, `stats.mapped`, so the layout of `Base` and `BaseBlock` is identical
/// to the C structures.

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/ExtentHooks.h>
#include <allocator/Mutex.h>
#include <allocator/Pages.h>
#include <allocator/SizeClassConstants.h>

namespace jemalloc
{

class ThreadState;

/// Alignment when THP is not enabled. Set to constant 2M in case the HUGEPAGE value is unexpected high (which would
/// cause VM over-reservation).
/// jemalloc: BASE_BLOCK_MIN_ALIGN
inline constexpr size_t BASE_BLOCK_MIN_ALIGN = size_t(2) << 20;

/// In auto mode, arenas switch to huge pages for the base allocator on the second base block. a0 switches to thp on
/// the 5th block (after 20 megabytes of metadata), since more metadata (e.g. rtree nodes) come from a0's base.
/// jemalloc: BASE_AUTO_THP_THRESHOLD, BASE_AUTO_THP_THRESHOLD_A0
inline constexpr size_t BASE_AUTO_TRANSPARENT_HUGE_PAGES_THRESHOLD = 2;
inline constexpr size_t BASE_AUTO_TRANSPARENT_HUGE_PAGES_THRESHOLD_A0 = 5;

/// Embedded at the beginning of every block of base-managed virtual memory.
/// jemalloc: base_block_t
struct BaseBlock
{
    /// Total size of block's virtual memory mapping.
    size_t size;

    /// Next block in list of base's blocks.
    BaseBlock * next;

    /// Tracks unused trailing space.
    Extent extent;
};

/// Measured from the C build.
static_assert(sizeof(BaseBlock) == (LOG2_PAGE == 12 ? 144 : (LOG2_PAGE == 14 ? 344 : 1128)));

/// jemalloc: base_t
class Base
{
public:
    constexpr Base() = default;

    Base(const Base &) = delete;
    Base & operator=(const Base &) = delete;

    /// Create a new base (placed into its own first block). Returns nullptr on failure.
    /// jemalloc: base_new
    static Base * create(ThreadState * thread_state, unsigned idx, const extent_hooks_t * extent_hooks_ptr, bool metadata_use_hooks);

    /// Release all blocks (with `opt_retain` they are not unmapped but purged).
    /// jemalloc: base_delete
    void destroy(ThreadState * thread_state);

    /// jemalloc: base_ind_get
    unsigned idxGet() const { return extent_hooks.idxGet(); }

    /// jemalloc: base_ehooks_get
    ExtentHooks * extentHooksGet() { return &extent_hooks; }

    /// jemalloc: base_ehooks_get_for_metadata
    ExtentHooks * extentHooksGetForMetadata() { return &extent_hooks_base; }

    /// Returns the old table.
    /// jemalloc: base_extent_hooks_set
    extent_hooks_t * extentHooksSet(extent_hooks_t * extent_hooks_ptr);

    /// Returns zeroed memory (demand-zeroed for the auto arenas, in order to make multi-page sparse data structures
    /// such as radix tree nodes efficient with respect to physical memory usage), at least `size` bytes with the
    /// specified alignment, or nullptr. `size` is rounded up to a multiple of the alignment to avoid false sharing.
    /// jemalloc: base_alloc
    void * alloc(ThreadState * thread_state, size_t size, size_t alignment);

    /// An `EXTENT_ALIGNMENT`-aligned `Extent` with `struct_serial_number` set.
    /// jemalloc: base_alloc_edata
    Extent * allocExtent(ThreadState * thread_state);

    /// CACHELINE-aligned memory for rtree nodes and leaves.
    /// jemalloc: base_alloc_rtree
    void * allocRadixTree(ThreadState * thread_state, size_t size);

    /// jemalloc: base_stats_get
    void statsGet(
        ThreadState * thread_state,
        size_t * allocated_,
        size_t * extent_allocated_,
        size_t * radix_tree_allocated_,
        size_t * resident_,
        size_t * mapped_,
        size_t * num_transparent_huge_pages_);

    /// jemalloc: base_prefork
    void prefork(ThreadState * thread_state) { mutex.prefork(thread_state); }
    /// jemalloc: base_postfork_parent
    void postforkParent(ThreadState * thread_state) { mutex.postforkParent(thread_state); }
    /// jemalloc: base_postfork_child
    void postforkChild(ThreadState * thread_state) { mutex.postforkChild(thread_state); }

    /// Introspection (tests).
    const BaseBlock * blocksList() const { return blocks; }

    /// The base mutex (the arena reads its profiling data for the stats and locks b0's mutex in `arenaInitHuge`).
    Mutex & getMutex() { return mutex; }

private:
    friend void * b0AllocThreadCacheStack(ThreadState * thread_state, size_t stack_size);
    friend void b0DeallocateThreadCacheStack(ThreadState * thread_state, void * thread_cache_stack);

    static void * map(ThreadState * thread_state, ExtentHooks * extent_hooks, unsigned idx, size_t size);
    static void unmap(ThreadState * thread_state, ExtentHooks * extent_hooks, unsigned idx, void * addr, size_t size);
    static BaseBlock * blockAlloc(
        ThreadState * thread_state,
        Base * base,
        ExtentHooks * extent_hooks,
        unsigned idx,
        PageSizeClassIdx * page_size_class_idx_last,
        size_t * extent_serial_number_next,
        size_t size,
        size_t alignment);
    static void * extentBumpAllocHelper(Extent * extent, size_t * gap_size, size_t size, size_t alignment);

    size_t getNumBlocks(bool with_new_block) const;
    void autoTransparentHugePagesSwitch(ThreadState * thread_state);
    void extentHeapInsert(ThreadState * thread_state, Extent * extent);
    Extent * allocBaseExtent(ThreadState * thread_state);
    void extentBumpAllocPost(ThreadState * thread_state, Extent * extent, size_t gap_size, void * addr, size_t size);
    void * extentBumpAlloc(ThreadState * thread_state, Extent * extent, size_t size, size_t alignment);
    Extent * extentAlloc(ThreadState * thread_state, size_t size, size_t alignment);
    void * allocImpl(ThreadState * thread_state, size_t size, size_t alignment, size_t * struct_serial_number, size_t * result_usable_size);

    /// User-configurable extent hook functions.
    ExtentHooks extent_hooks;

    /// User-configurable extent hook functions for metadata allocations.
    ExtentHooks extent_hooks_base;

    /// Protects `alloc` and `statsGet` operations.
    Mutex mutex;

    /// Using THP when true (metadata_thp auto mode).
    bool auto_transparent_huge_pages_switched = false;

    /// Most recent size class in the series of increasingly large base extents. Logarithmic spacing between
    /// subsequent allocations ensures that the total number of distinct mappings remains small.
    PageSizeClassIdx page_size_class_idx_last = 0;

    /// Serial number generation state.
    size_t extent_serial_number_next = 0;

    /// Chain of all blocks associated with base.
    BaseBlock * blocks = nullptr;

    /// Heap of extents that track unused trailing space within blocks.
    ExtentHeap available[SIZE_CLASS_NUM_SIZES];

    /// Contains reusable base edata (used by tcache stacks currently).
    ExtentAvailableHeap extent_available;

    /// Stats.
    size_t allocated = 0;
    size_t extent_allocated = 0;
    size_t radix_tree_allocated = 0;
    size_t resident = 0;
    size_t mapped = 0;
    /// Number of THP regions touched.
    size_t num_transparent_huge_pages = 0;
};

/// The base of arena 0.
/// jemalloc: b0get
Base * base0Get();

/// Each piece allocated here is managed by a separate `Extent`, because it was bump allocated and cannot be merged
/// back into the original block. This means it's not for general purpose: 1) they are not page aligned, nor page
/// sized, and 2) the requested size should not be too small (as each piece comes with an `Extent`). Only used for
/// tcache bin stack allocation.
/// jemalloc: b0_alloc_tcache_stack
void * b0AllocThreadCacheStack(ThreadState * thread_state, size_t stack_size);

/// jemalloc: b0_dalloc_tcache_stack
void b0DeallocateThreadCacheStack(ThreadState * thread_state, void * thread_cache_stack);

/// Creates `b0`. Returns true on error.
/// jemalloc: base_boot
bool baseBoot(ThreadState * thread_state);

}
