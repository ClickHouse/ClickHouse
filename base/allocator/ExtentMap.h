#pragma once

/// The extent map: page address -> extent metadata, on top of the radix tree (jemalloc: `emap.h`, `emap.c`, and the
/// neighbor acquisition rules from `extent.h`).
///
/// Invariants (see `03-extents-extent_map-base.md` 7.1): every extent known to the page allocator has its first and last
/// page registered; active slabs additionally have every interior page registered; the rtree `state` mirrors
/// `Extent::state` and is the synchronization token for neighbor acquisition.

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/Options.h>
#include <allocator/RadixTree.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

namespace jemalloc
{

class Base;

/// Used to pass rtree lookup context down the path. jemalloc: emap_alloc_ctx_t
struct AllocContext
{
    size_t usable_size;
    SizeClassIdx size_class_idx;
    bool slab;

    /// jemalloc: emap_alloc_ctx_init
    ALLOCATOR_ALWAYS_INLINE void init(SizeClassIdx size_class_idx_, bool slab_, size_t usable_size_)
    {
        size_class_idx = size_class_idx_;
        slab = slab_;
        usable_size = usable_size_;
        ALLOCATOR_ASSERT(size_classes::largeSizeClassesDisabled() || usable_size == size_classes::indexToSize(size_class_idx));
    }

    /// jemalloc: emap_alloc_ctx_usize_get
    ALLOCATOR_ALWAYS_INLINE size_t usableSizeGet() const
    {
        ALLOCATOR_ASSERT(size_class_idx < SIZE_CLASS_NUM_SIZES);
        if (slab)
        {
            ALLOCATOR_ASSERT(usable_size == size_classes::indexToSize(size_class_idx));
            return size_classes::indexToSize(size_class_idx);
        }
        ALLOCATOR_ASSERT(size_classes::largeSizeClassesDisabled() || usable_size == size_classes::indexToSize(size_class_idx));
        ALLOCATOR_ASSERT(usable_size <= SIZE_CLASS_LARGE_MAX_CLASS);
        return usable_size;
    }
};

/// jemalloc: emap_full_alloc_ctx_t
struct FullAllocContext
{
    SizeClassIdx size_class_idx;
    bool slab;
    Extent * extent;
};

/// jemalloc: emap_prepare_t
struct ExtentMapPrepare
{
    RadixTreeLeafElement * lead_element_a;
    RadixTreeLeafElement * lead_element_b;
    RadixTreeLeafElement * trail_element_a;
    RadixTreeLeafElement * trail_element_b;
};

/// For batch lookups out of the cache bins (which invert the usual ordering in deciding what to flush).
/// jemalloc: emap_ptr_getter, emap_metadata_visitor
using ExtentMapPtrGetter = const void * (*)(void * context, size_t idx);
using ExtentMapMetadataVisitor = void (*)(void * context, FullAllocContext * alloc_context);

/// jemalloc: emap_batch_lookup_result_t
union ExtentMapBatchLookupResult
{
    Extent * extent;
    RadixTreeLeafElement * radix_tree_leaf;
};

/// Head states checking: disallow merging if the higher addr extent is a head extent. This helps preserve first-fit,
/// and more importantly makes sure no merge across arenas.
/// jemalloc: extent_neighbor_head_state_mergeable
ALLOCATOR_ALWAYS_INLINE bool extentNeighborHeadStateMergeable(bool extent_is_head, bool neighbor_is_head, bool forward)
{
    if (forward)
    {
        if (neighbor_is_head)
            return false;
    }
    else
    {
        if (extent_is_head)
            return false;
    }
    return true;
}

/// jemalloc: extent_can_acquire_neighbor
ALLOCATOR_ALWAYS_INLINE bool extentCanAcquireNeighbor(
    Extent * extent,
    RadixTreeContents contents,
    ExtentAllocatorKind allocator_kind,
    ExtentState expected_state,
    bool forward,
    bool expanding)
{
    Extent * neighbor = contents.extent;
    if (neighbor == nullptr)
        return false;
    /// It's not safe to access `*neighbor` yet; must verify states first.
    bool neighbor_is_head = contents.metadata.is_head;
    if (!extentNeighborHeadStateMergeable(extent->isHead(), neighbor_is_head, forward))
        return false;
    ExtentState neighbor_state = contents.metadata.state;
    if (allocator_kind == EXTENT_ALLOCATOR_PAGE_ALLOCATOR)
    {
        if (neighbor_state != expected_state)
            return false;
        /// From this point, it's safe to access `*neighbor`.
        if (!expanding && (extent->committed() != neighbor->committed()))
        {
            /// Some platforms (e.g. Windows) require an explicit commit step (and writing to uncommitted memory is not
            /// allowed).
            return false;
        }
    }
    else
    {
        if (neighbor_state == extent_state_active)
            return false;
        /// From this point, it's safe to access `*neighbor`.
    }

    ALLOCATOR_ASSERT(extent->allocatorKind() == allocator_kind);
    if (neighbor->allocatorKind() != allocator_kind)
        return false;
    if (options.retain)
    {
        ALLOCATOR_ASSERT(extent->arenaIdx() == neighbor->arenaIdx());
    }
    else
    {
        if (extent->arenaIdx() != neighbor->arenaIdx())
            return false;
    }
    ALLOCATOR_ASSERT(!extent->guarded() && !neighbor->guarded());

    return true;
}

/// jemalloc: extent_assert_can_coalesce
ALLOCATOR_ALWAYS_INLINE void extentAssertCanCoalesce([[maybe_unused]] const Extent * inner, [[maybe_unused]] const Extent * outer)
{
    ALLOCATOR_ASSERT(inner->arenaIdx() == outer->arenaIdx());
    ALLOCATOR_ASSERT(inner->allocatorKind() == outer->allocatorKind());
    ALLOCATOR_ASSERT(inner->committed() == outer->committed());
    ALLOCATOR_ASSERT(inner->state() == extent_state_active);
    ALLOCATOR_ASSERT(outer->state() == extent_state_merging);
    ALLOCATOR_ASSERT(!inner->guarded() && !outer->guarded());
    ALLOCATOR_ASSERT(inner->base() == outer->past() || outer->base() == inner->past());
}

/// jemalloc: extent_assert_can_expand
ALLOCATOR_ALWAYS_INLINE void extentAssertCanExpand([[maybe_unused]] const Extent * original, [[maybe_unused]] const Extent * expand)
{
    ALLOCATOR_ASSERT(original->arenaIdx() == expand->arenaIdx());
    ALLOCATOR_ASSERT(original->allocatorKind() == expand->allocatorKind());
    ALLOCATOR_ASSERT(original->state() == extent_state_active);
    ALLOCATOR_ASSERT(expand->state() == extent_state_merging);
    ALLOCATOR_ASSERT(original->past() == expand->base());
}

/// jemalloc: emap_t
class ExtentMap
{
public:
    constexpr ExtentMap() = default;

    ExtentMap(const ExtentMap &) = delete;
    ExtentMap & operator=(const ExtentMap &) = delete;

    /// Returns true on error.
    /// jemalloc: emap_init
    bool init(Base * base, bool zeroed) { return radix_tree.init(base, zeroed); }

    /// Changes the szind and slab status of an extent's boundary mappings. If the extent is not a slab, the end
    /// mapping is not updated (lookups only occur in the interior of an extent for slabs). Since szind and slab only
    /// make sense for active extents, this is only called while activating or deactivating an extent.
    /// No-op if `size_class_idx == SIZE_CLASS_NUM_SIZES`.
    /// jemalloc: emap_remap
    void remap(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx, bool slab);

    /// Requires a core lock to be held.
    /// jemalloc: emap_update_edata_state
    void updateExtentState(ThreadState * thread_state, Extent * extent, ExtentState state);

    /// The two acquire functions allow accessing neighbor extents, if it's safe and valid to do so (i.e. from the
    /// same arena, of the same state, etc.). This is necessary because the ecache locks are state based, and only
    /// protect extents with the same state, so the neighbor's state must be verified first, before chasing the
    /// pointer. The returned extent is in an acquired state (`merging`), so other threads won't access it even though
    /// it can still be discovered from the rtree. The acquire operation itself is done under the state locks.
    /// jemalloc: emap_try_acquire_edata_neighbor
    Extent * tryAcquireExtentNeighbor(
        ThreadState * thread_state, Extent * extent, ExtentAllocatorKind allocator_kind, ExtentState expected_state, bool forward);

    /// Tries expanding forward.
    /// jemalloc: emap_try_acquire_edata_neighbor_expand
    Extent * tryAcquireExtentNeighborExpand(
        ThreadState * thread_state, Extent * extent, ExtentAllocatorKind allocator_kind, ExtentState expected_state);

    /// jemalloc: emap_release_edata
    void releaseExtent(ThreadState * thread_state, Extent * extent, ExtentState new_state);

    /// Associates the extent with its beginning and end address, setting szind and slab. Returns true on error
    /// (resource exhaustion).
    /// jemalloc: emap_register_boundary
    bool registerBoundary(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx, bool slab);

    /// The same for the interior of the range, for slab allocations; invoked *after* `registerBoundary`. Can't fail:
    /// slabs can't get big enough to touch a new leaf that neither of the boundaries touched.
    /// jemalloc: emap_register_interior
    void registerInterior(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx);

    /// jemalloc: emap_deregister_boundary
    void deregisterBoundary(ThreadState * thread_state, Extent * extent);

    /// jemalloc: emap_deregister_interior
    void deregisterInterior(ThreadState * thread_state, Extent * extent);

    /// Split and merge have a "prepare" part, which can be done without exclusive access to the extent, and a
    /// "commit" part, which requires exclusive access. Only `splitPrepare` can fail (returns true on failure, then the
    /// caller must not commit). "lead" is the lower-addressed extent, "trail" the higher-addressed one. The caller
    /// sets the extent states.
    /// jemalloc: emap_split_prepare
    bool
    splitPrepare(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * extent, size_t size_a, Extent * trail, size_t size_b);

    /// jemalloc: emap_split_commit
    void splitCommit(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, size_t size_a, Extent * trail, size_t size_b);

    /// jemalloc: emap_merge_prepare
    void mergePrepare(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, Extent * trail);

    /// jemalloc: emap_merge_commit
    void mergeCommit(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, Extent * trail);

    /// Asserts that the emap's view of the extent matches the extent's view (debug only).
    /// jemalloc: emap_assert_mapped, emap_do_assert_mapped
    ALLOCATOR_ALWAYS_INLINE void assertMapped(ThreadState * thread_state, Extent * extent)
    {
        if constexpr (config::debug)
            doAssertMapped(thread_state, extent);
    }

    /// Asserts that the extent isn't in the map (debug only).
    /// jemalloc: emap_assert_not_mapped, emap_do_assert_not_mapped
    ALLOCATOR_ALWAYS_INLINE void assertNotMapped(ThreadState * thread_state, Extent * extent)
    {
        if constexpr (config::debug)
            doAssertNotMapped(thread_state, extent);
    }

    /// Debug only.
    /// jemalloc: emap_edata_in_transition
    ALLOCATOR_ALWAYS_INLINE bool extentInTransition(ThreadState * thread_state, Extent * extent)
    {
        ALLOCATOR_ASSERT(config::debug);
        assertMapped(thread_state, extent);

        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
        RadixTreeContents contents = radix_tree.read(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->base()));

        return extentStateInTransition(contents.metadata.state);
    }

    /// The extent is considered acquired if no other threads will attempt to read / write any fields from it:
    /// 1) it is not hooked into the emap yet (just allocated or initialized), or
    /// 2) it is in an active or transition state: it can be discovered from the emap, but the state tracked in the
    ///    rtree prevents other threads from accessing it.
    /// For assertions only (always false in release builds).
    /// jemalloc: emap_edata_is_acquired
    ALLOCATOR_ALWAYS_INLINE bool extentIsAcquired(ThreadState * thread_state, Extent * extent)
    {
        if constexpr (!config::debug)
            return false;

        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
        RadixTreeLeafElement * element = radix_tree.leafElementLookup(
            thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->base()), /* dependent */ false, /* init_missing */ false);
        if (element == nullptr)
            return true;
        RadixTreeContents contents = RadixTree::leafElementRead(thread_state, element, /* dependent */ false);
        if (contents.extent == nullptr || contents.metadata.state == extent_state_active
            || extentStateInTransition(contents.metadata.state))
            return true;

        return false;
    }

    /// --- Lookups ---------------------------------------------------------------------------------------------------

    /// jemalloc: emap_edata_lookup
    ALLOCATOR_ALWAYS_INLINE Extent * extentLookup(ThreadState * thread_state, const void * ptr)
    {
        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

        return radix_tree.read(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr)).extent;
    }

    /// Fills in `alloc_context` with the info in the map.
    /// jemalloc: emap_alloc_ctx_lookup
    ALLOCATOR_ALWAYS_INLINE void allocContextLookup(ThreadState * thread_state, const void * ptr, AllocContext * alloc_context)
    {
        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

        RadixTreeContents contents = radix_tree.read(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr));
        /// If the alloc is invalid, do not calculate usize since the extent could be corrupted.
        alloc_context->init(
            contents.metadata.size_class_idx,
            contents.metadata.slab,
            (contents.metadata.size_class_idx == SIZE_CLASS_NUM_SIZES || contents.extent == nullptr) ? 0 : contents.extent->usableSize());
    }

    /// The pointer must be mapped.
    /// jemalloc: emap_full_alloc_ctx_lookup
    ALLOCATOR_ALWAYS_INLINE void fullAllocContextLookup(ThreadState * thread_state, const void * ptr, FullAllocContext * full_alloc_context)
    {
        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

        RadixTreeContents contents = radix_tree.read(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr));
        full_alloc_context->extent = contents.extent;
        full_alloc_context->size_class_idx = contents.metadata.size_class_idx;
        full_alloc_context->slab = contents.metadata.slab;
    }

    /// The pointer is allowed to not be mapped. Returns true when the pointer is not present.
    /// jemalloc: emap_full_alloc_ctx_try_lookup
    ALLOCATOR_ALWAYS_INLINE bool
    fullAllocContextTryLookup(ThreadState * thread_state, const void * ptr, FullAllocContext * full_alloc_context)
    {
        RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

        RadixTreeContents contents;
        bool error = radix_tree.readIndependent(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr), &contents);
        if (error)
            return true;
        full_alloc_context->extent = contents.extent;
        full_alloc_context->size_class_idx = contents.metadata.size_class_idx;
        full_alloc_context->slab = contents.metadata.slab;
        return false;
    }

    /// Only used on the fast path of free. Returns true when it cannot be fulfilled by the fast path, e.g. when the
    /// metadata key is not cached (L1 only).
    /// jemalloc: emap_alloc_ctx_try_lookup_fast
    ALLOCATOR_ALWAYS_INLINE bool allocContextTryLookupFast(ThreadState & thread_state, const void * ptr, AllocContext * alloc_context)
    {
        /// Use the unsafe getter since this may get called during exit.
        RadixTreeContext * radix_tree_context = thread_state.radixTreeContext();

        RadixTreeMetadata metadata;
        bool error = radix_tree.metadataTryReadFast(&thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr), &metadata);
        if (error)
            return true;
        /// Small allocs using the fast path can always use the index to get the usize. Therefore, do not set
        /// `alloc_context->usable_size` here.
        alloc_context->size_class_idx = metadata.size_class_idx;
        alloc_context->slab = metadata.slab;
        if constexpr (config::debug)
            alloc_context->usable_size = SIZE_CLASS_LARGE_MAX_CLASS + 1;
        return false;
    }

    /// Two passes: first all the leaf lookups (into the result array, reused as a temporary buffer), then reading the
    /// contents and calling the visitor (which allows size-checking assertions).
    /// jemalloc: emap_edata_lookup_batch
    ALLOCATOR_ALWAYS_INLINE void extentLookupBatch(
        ThreadState & thread_state,
        size_t num_ptrs,
        ExtentMapPtrGetter ptr_getter,
        void * ptr_getter_context,
        ExtentMapMetadataVisitor metadata_visitor,
        void * metadata_visitor_context,
        ExtentMapBatchLookupResult * result)
    {
        RadixTreeContext * radix_tree_context = thread_state.radixTreeContext();

        for (size_t i = 0; i < num_ptrs; ++i)
        {
            const void * ptr = ptr_getter(ptr_getter_context, i);
            result[i].radix_tree_leaf = radix_tree.leafElementLookup(
                &thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(ptr), /* dependent */ true, /* init_missing */ false);
        }

        for (size_t i = 0; i < num_ptrs; ++i)
        {
            RadixTreeLeafElement * element = result[i].radix_tree_leaf;
            RadixTreeContents contents = RadixTree::leafElementRead(&thread_state, element, /* dependent */ true);
            result[i].extent = contents.extent;
            FullAllocContext alloc_context;
            /// Not all these fields are read in practice by the metadata visitor, but the compiler can easily
            /// optimize away the ones that aren't.
            alloc_context.size_class_idx = contents.metadata.size_class_idx;
            alloc_context.slab = contents.metadata.slab;
            alloc_context.extent = contents.extent;
            metadata_visitor(metadata_visitor_context, &alloc_context);
        }
    }

    RadixTree radix_tree;

private:
    /// jemalloc: emap_try_acquire_edata_neighbor_impl
    Extent * tryAcquireExtentNeighborImpl(
        ThreadState * thread_state,
        Extent * extent,
        ExtentAllocatorKind allocator_kind,
        ExtentState expected_state,
        bool forward,
        bool expanding);

    /// Returns true on lookup failure (only possible if `!dependent`).
    /// jemalloc: emap_rtree_leaf_elms_lookup
    bool radixTreeLeafElementsLookup(
        ThreadState * thread_state,
        RadixTreeContext * radix_tree_context,
        const Extent * extent,
        bool dependent,
        bool init_missing,
        RadixTreeLeafElement ** r_element_a,
        RadixTreeLeafElement ** r_element_b);

    /// jemalloc: emap_rtree_write_acquired
    void radixTreeWriteAcquired(
        ThreadState * thread_state,
        RadixTreeLeafElement * element_a,
        RadixTreeLeafElement * element_b,
        Extent * extent,
        SizeClassIdx size_class_idx,
        bool slab);

    void doAssertMapped(ThreadState * thread_state, Extent * extent);
    void doAssertNotMapped(ThreadState * thread_state, Extent * extent);
};

/// The global extent map: zero-initialized (it is large: the rtree root array lives in it), initialized with
/// `arena_extent_map_global.init(base0Get(), true)` at boot.
/// jemalloc: arena_emap_global
extern constinit ExtentMap arena_extent_map_global;

}
