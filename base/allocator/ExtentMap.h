#pragma once

/// The extent map: page address -> extent metadata, on top of the radix tree (jemalloc: `emap.h`, `emap.c`, and the
/// neighbor acquisition rules from `extent.h`).
///
/// Invariants (see `03-extents-emap-base.md` 7.1): every extent known to the page allocator has its first and last
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
    size_t usize;
    szind_t szind;
    bool slab;

    /// jemalloc: emap_alloc_ctx_init
    JE_ALWAYS_INLINE void init(szind_t szind_, bool slab_, size_t usize_)
    {
        szind = szind_;
        slab = slab_;
        usize = usize_;
        JE_ASSERT(sz::largeSizeClassesDisabled() || usize == sz::indexToSize(szind));
    }

    /// jemalloc: emap_alloc_ctx_usize_get
    JE_ALWAYS_INLINE size_t usizeGet() const
    {
        JE_ASSERT(szind < SC_NSIZES);
        if (slab)
        {
            JE_ASSERT(usize == sz::indexToSize(szind));
            return sz::indexToSize(szind);
        }
        JE_ASSERT(sz::largeSizeClassesDisabled() || usize == sz::indexToSize(szind));
        JE_ASSERT(usize <= SC_LARGE_MAXCLASS);
        return usize;
    }
};

/// jemalloc: emap_full_alloc_ctx_t
struct FullAllocContext
{
    szind_t szind;
    bool slab;
    Extent * edata;
};

/// jemalloc: emap_prepare_t
struct ExtentMapPrepare
{
    RadixTreeLeafElm * lead_elm_a;
    RadixTreeLeafElm * lead_elm_b;
    RadixTreeLeafElm * trail_elm_a;
    RadixTreeLeafElm * trail_elm_b;
};

/// For batch lookups out of the cache bins (which invert the usual ordering in deciding what to flush).
/// jemalloc: emap_ptr_getter, emap_metadata_visitor
using ExtentMapPtrGetter = const void * (*)(void * ctx, size_t ind);
using ExtentMapMetadataVisitor = void (*)(void * ctx, FullAllocContext * alloc_ctx);

/// jemalloc: emap_batch_lookup_result_t
union ExtentMapBatchLookupResult
{
    Extent * edata;
    RadixTreeLeafElm * rtree_leaf;
};

/// Head states checking: disallow merging if the higher addr extent is a head extent. This helps preserve first-fit,
/// and more importantly makes sure no merge across arenas.
/// jemalloc: extent_neighbor_head_state_mergeable
JE_ALWAYS_INLINE bool extentNeighborHeadStateMergeable(bool edata_is_head, bool neighbor_is_head, bool forward)
{
    if (forward)
    {
        if (neighbor_is_head)
            return false;
    }
    else
    {
        if (edata_is_head)
            return false;
    }
    return true;
}

/// jemalloc: extent_can_acquire_neighbor
JE_ALWAYS_INLINE bool extentCanAcquireNeighbor(
    Extent * edata, RadixTreeContents contents, ExtentPai pai, ExtentState expected_state, bool forward, bool expanding)
{
    Extent * neighbor = contents.edata;
    if (neighbor == nullptr)
        return false;
    /// It's not safe to access `*neighbor` yet; must verify states first.
    bool neighbor_is_head = contents.metadata.is_head;
    if (!extentNeighborHeadStateMergeable(edata->isHead(), neighbor_is_head, forward))
        return false;
    ExtentState neighbor_state = contents.metadata.state;
    if (pai == EXTENT_PAI_PAC)
    {
        if (neighbor_state != expected_state)
            return false;
        /// From this point, it's safe to access `*neighbor`.
        if (!expanding && (edata->committed() != neighbor->committed()))
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

    JE_ASSERT(edata->pai() == pai);
    if (neighbor->pai() != pai)
        return false;
    if (opt.retain)
    {
        JE_ASSERT(edata->arenaInd() == neighbor->arenaInd());
    }
    else
    {
        if (edata->arenaInd() != neighbor->arenaInd())
            return false;
    }
    JE_ASSERT(!edata->guarded() && !neighbor->guarded());

    return true;
}

/// jemalloc: extent_assert_can_coalesce
JE_ALWAYS_INLINE void extentAssertCanCoalesce([[maybe_unused]] const Extent * inner, [[maybe_unused]] const Extent * outer)
{
    JE_ASSERT(inner->arenaInd() == outer->arenaInd());
    JE_ASSERT(inner->pai() == outer->pai());
    JE_ASSERT(inner->committed() == outer->committed());
    JE_ASSERT(inner->state() == extent_state_active);
    JE_ASSERT(outer->state() == extent_state_merging);
    JE_ASSERT(!inner->guarded() && !outer->guarded());
    JE_ASSERT(inner->base() == outer->past() || outer->base() == inner->past());
}

/// jemalloc: extent_assert_can_expand
JE_ALWAYS_INLINE void extentAssertCanExpand([[maybe_unused]] const Extent * original, [[maybe_unused]] const Extent * expand)
{
    JE_ASSERT(original->arenaInd() == expand->arenaInd());
    JE_ASSERT(original->pai() == expand->pai());
    JE_ASSERT(original->state() == extent_state_active);
    JE_ASSERT(expand->state() == extent_state_merging);
    JE_ASSERT(original->past() == expand->base());
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
    bool init(Base * base, bool zeroed) { return rtree.init(base, zeroed); }

    /// Changes the szind and slab status of an extent's boundary mappings. If the extent is not a slab, the end
    /// mapping is not updated (lookups only occur in the interior of an extent for slabs). Since szind and slab only
    /// make sense for active extents, this is only called while activating or deactivating an extent.
    /// No-op if `szind == SC_NSIZES`.
    /// jemalloc: emap_remap
    void remap(ThreadState * tsdn, Extent * edata, szind_t szind, bool slab);

    /// Requires a core lock to be held.
    /// jemalloc: emap_update_edata_state
    void updateEdataState(ThreadState * tsdn, Extent * edata, ExtentState state);

    /// The two acquire functions allow accessing neighbor extents, if it's safe and valid to do so (i.e. from the
    /// same arena, of the same state, etc.). This is necessary because the ecache locks are state based, and only
    /// protect extents with the same state, so the neighbor's state must be verified first, before chasing the
    /// pointer. The returned extent is in an acquired state (`merging`), so other threads won't access it even though
    /// it can still be discovered from the rtree. The acquire operation itself is done under the state locks.
    /// jemalloc: emap_try_acquire_edata_neighbor
    Extent * tryAcquireEdataNeighbor(ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state, bool forward);

    /// Tries expanding forward.
    /// jemalloc: emap_try_acquire_edata_neighbor_expand
    Extent * tryAcquireEdataNeighborExpand(ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state);

    /// jemalloc: emap_release_edata
    void releaseEdata(ThreadState * tsdn, Extent * edata, ExtentState new_state);

    /// Associates the extent with its beginning and end address, setting szind and slab. Returns true on error
    /// (resource exhaustion).
    /// jemalloc: emap_register_boundary
    bool registerBoundary(ThreadState * tsdn, Extent * edata, szind_t szind, bool slab);

    /// The same for the interior of the range, for slab allocations; invoked *after* `registerBoundary`. Can't fail:
    /// slabs can't get big enough to touch a new leaf that neither of the boundaries touched.
    /// jemalloc: emap_register_interior
    void registerInterior(ThreadState * tsdn, Extent * edata, szind_t szind);

    /// jemalloc: emap_deregister_boundary
    void deregisterBoundary(ThreadState * tsdn, Extent * edata);

    /// jemalloc: emap_deregister_interior
    void deregisterInterior(ThreadState * tsdn, Extent * edata);

    /// Split and merge have a "prepare" part, which can be done without exclusive access to the extent, and a
    /// "commit" part, which requires exclusive access. Only `splitPrepare` can fail (returns true on failure, then the
    /// caller must not commit). "lead" is the lower-addressed extent, "trail" the higher-addressed one. The caller
    /// sets the extent states.
    /// jemalloc: emap_split_prepare
    bool splitPrepare(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * edata, size_t size_a, Extent * trail, size_t size_b);

    /// jemalloc: emap_split_commit
    void splitCommit(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, size_t size_a, Extent * trail, size_t size_b);

    /// jemalloc: emap_merge_prepare
    void mergePrepare(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, Extent * trail);

    /// jemalloc: emap_merge_commit
    void mergeCommit(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, Extent * trail);

    /// Asserts that the emap's view of the extent matches the extent's view (debug only).
    /// jemalloc: emap_assert_mapped, emap_do_assert_mapped
    JE_ALWAYS_INLINE void assertMapped(ThreadState * tsdn, Extent * edata)
    {
        if constexpr (config::debug)
            doAssertMapped(tsdn, edata);
    }

    /// Asserts that the extent isn't in the map (debug only).
    /// jemalloc: emap_assert_not_mapped, emap_do_assert_not_mapped
    JE_ALWAYS_INLINE void assertNotMapped(ThreadState * tsdn, Extent * edata)
    {
        if constexpr (config::debug)
            doAssertNotMapped(tsdn, edata);
    }

    /// Debug only.
    /// jemalloc: emap_edata_in_transition
    JE_ALWAYS_INLINE bool edataInTransition(ThreadState * tsdn, Extent * edata)
    {
        JE_ASSERT(config::debug);
        assertMapped(tsdn, edata);

        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
        RadixTreeContents contents = rtree.read(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()));

        return extentStateInTransition(contents.metadata.state);
    }

    /// The extent is considered acquired if no other threads will attempt to read / write any fields from it:
    /// 1) it is not hooked into the emap yet (just allocated or initialized), or
    /// 2) it is in an active or transition state: it can be discovered from the emap, but the state tracked in the
    ///    rtree prevents other threads from accessing it.
    /// For assertions only (always false in release builds).
    /// jemalloc: emap_edata_is_acquired
    JE_ALWAYS_INLINE bool edataIsAcquired(ThreadState * tsdn, Extent * edata)
    {
        if constexpr (!config::debug)
            return false;

        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
        RadixTreeLeafElm * elm = rtree.leafElmLookup(
            tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()), /* dependent */ false, /* init_missing */ false);
        if (elm == nullptr)
            return true;
        RadixTreeContents contents = RadixTree::leafElmRead(tsdn, elm, /* dependent */ false);
        if (contents.edata == nullptr || contents.metadata.state == extent_state_active
            || extentStateInTransition(contents.metadata.state))
            return true;

        return false;
    }

    /// --- Lookups ---------------------------------------------------------------------------------------------------

    /// jemalloc: emap_edata_lookup
    JE_ALWAYS_INLINE Extent * edataLookup(ThreadState * tsdn, const void * ptr)
    {
        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

        return rtree.read(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(ptr)).edata;
    }

    /// Fills in `alloc_ctx` with the info in the map.
    /// jemalloc: emap_alloc_ctx_lookup
    JE_ALWAYS_INLINE void allocCtxLookup(ThreadState * tsdn, const void * ptr, AllocContext * alloc_ctx)
    {
        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

        RadixTreeContents contents = rtree.read(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(ptr));
        /// If the alloc is invalid, do not calculate usize since the extent could be corrupted.
        alloc_ctx->init(
            contents.metadata.szind,
            contents.metadata.slab,
            (contents.metadata.szind == SC_NSIZES || contents.edata == nullptr) ? 0 : contents.edata->usize());
    }

    /// The pointer must be mapped.
    /// jemalloc: emap_full_alloc_ctx_lookup
    JE_ALWAYS_INLINE void fullAllocCtxLookup(ThreadState * tsdn, const void * ptr, FullAllocContext * full_alloc_ctx)
    {
        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

        RadixTreeContents contents = rtree.read(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(ptr));
        full_alloc_ctx->edata = contents.edata;
        full_alloc_ctx->szind = contents.metadata.szind;
        full_alloc_ctx->slab = contents.metadata.slab;
    }

    /// The pointer is allowed to not be mapped. Returns true when the pointer is not present.
    /// jemalloc: emap_full_alloc_ctx_try_lookup
    JE_ALWAYS_INLINE bool fullAllocCtxTryLookup(ThreadState * tsdn, const void * ptr, FullAllocContext * full_alloc_ctx)
    {
        RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
        RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

        RadixTreeContents contents;
        bool err = rtree.readIndependent(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(ptr), &contents);
        if (err)
            return true;
        full_alloc_ctx->edata = contents.edata;
        full_alloc_ctx->szind = contents.metadata.szind;
        full_alloc_ctx->slab = contents.metadata.slab;
        return false;
    }

    /// Only used on the fast path of free. Returns true when it cannot be fulfilled by the fast path, e.g. when the
    /// metadata key is not cached (L1 only).
    /// jemalloc: emap_alloc_ctx_try_lookup_fast
    JE_ALWAYS_INLINE bool allocCtxTryLookupFast(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx)
    {
        /// Use the unsafe getter since this may get called during exit.
        RadixTreeContext * rtree_ctx = tsd.rtreeCtx();

        RadixTreeMetadata metadata;
        bool err = rtree.metadataTryReadFast(&tsd, rtree_ctx, reinterpret_cast<uintptr_t>(ptr), &metadata);
        if (err)
            return true;
        /// Small allocs using the fast path can always use the index to get the usize. Therefore, do not set
        /// `alloc_ctx->usize` here.
        alloc_ctx->szind = metadata.szind;
        alloc_ctx->slab = metadata.slab;
        if constexpr (config::debug)
            alloc_ctx->usize = SC_LARGE_MAXCLASS + 1;
        return false;
    }

    /// Two passes: first all the leaf lookups (into the result array, reused as a temporary buffer), then reading the
    /// contents and calling the visitor (which allows size-checking assertions).
    /// jemalloc: emap_edata_lookup_batch
    JE_ALWAYS_INLINE void edataLookupBatch(
        ThreadState & tsd,
        size_t nptrs,
        ExtentMapPtrGetter ptr_getter,
        void * ptr_getter_ctx,
        ExtentMapMetadataVisitor metadata_visitor,
        void * metadata_visitor_ctx,
        ExtentMapBatchLookupResult * result)
    {
        RadixTreeContext * rtree_ctx = tsd.rtreeCtx();

        for (size_t i = 0; i < nptrs; ++i)
        {
            const void * ptr = ptr_getter(ptr_getter_ctx, i);
            result[i].rtree_leaf = rtree.leafElmLookup(
                &tsd, rtree_ctx, reinterpret_cast<uintptr_t>(ptr), /* dependent */ true, /* init_missing */ false);
        }

        for (size_t i = 0; i < nptrs; ++i)
        {
            RadixTreeLeafElm * elm = result[i].rtree_leaf;
            RadixTreeContents contents = RadixTree::leafElmRead(&tsd, elm, /* dependent */ true);
            result[i].edata = contents.edata;
            FullAllocContext alloc_ctx;
            /// Not all these fields are read in practice by the metadata visitor, but the compiler can easily
            /// optimize away the ones that aren't.
            alloc_ctx.szind = contents.metadata.szind;
            alloc_ctx.slab = contents.metadata.slab;
            alloc_ctx.edata = contents.edata;
            metadata_visitor(metadata_visitor_ctx, &alloc_ctx);
        }
    }

    RadixTree rtree;

private:
    /// jemalloc: emap_try_acquire_edata_neighbor_impl
    Extent * tryAcquireEdataNeighborImpl(
        ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state, bool forward, bool expanding);

    /// Returns true on lookup failure (only possible if `!dependent`).
    /// jemalloc: emap_rtree_leaf_elms_lookup
    bool rtreeLeafElmsLookup(
        ThreadState * tsdn,
        RadixTreeContext * rtree_ctx,
        const Extent * edata,
        bool dependent,
        bool init_missing,
        RadixTreeLeafElm ** r_elm_a,
        RadixTreeLeafElm ** r_elm_b);

    /// jemalloc: emap_rtree_write_acquired
    void rtreeWriteAcquired(
        ThreadState * tsdn, RadixTreeLeafElm * elm_a, RadixTreeLeafElm * elm_b, Extent * edata, szind_t szind, bool slab);

    void doAssertMapped(ThreadState * tsdn, Extent * edata);
    void doAssertNotMapped(ThreadState * tsdn, Extent * edata);
};

/// The global extent map: zero-initialized (it is large: the rtree root array lives in it), initialized with
/// `arena_emap_global.init(b0get(), true)` at boot.
/// jemalloc: arena_emap_global
extern constinit ExtentMap arena_emap_global;

}
