#include <allocator/ExtentMap.h>

namespace jemalloc
{

constinit ExtentMap arena_emap_global;

void ExtentMap::updateEdataState(ThreadState * tsdn, Extent * edata, ExtentState state)
{
    edata->setState(state);

    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
    RadixTreeLeafElm * elm1 = rtree.leafElmLookup(
        tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()), /* dependent */ true, /* init_missing */ false);
    JE_ASSERT(elm1 != nullptr);
    RadixTreeLeafElm * elm2 = edata->size() == PAGE
        ? nullptr
        : rtree.leafElmLookup(
              tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->last()), /* dependent */ true, /* init_missing */ false);

    RadixTree::leafElmStateUpdate(tsdn, elm1, elm2, state);

    assertMapped(tsdn, edata);
}

Extent * ExtentMap::tryAcquireEdataNeighborImpl(
    ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state, bool forward, bool expanding)
{
    JE_ASSERT(!edata->guarded());
    JE_ASSERT(!expanding || forward);
    JE_ASSERT(!extentStateInTransition(expected_state));
    JE_ASSERT(expected_state == extent_state_dirty || expected_state == extent_state_muzzy || expected_state == extent_state_retained);

    void * neighbor_addr = forward ? edata->past() : edata->before();
    /// This is subtle; the rtree code asserts that its input pointer is non-null, and this is a useful thing to check.
    /// But it's possible that the extent corresponds to an address of `(void *)PAGE` (in practice, this has only been
    /// observed on FreeBSD when address-space randomization is on, but it could in principle happen anywhere). In this
    /// case, `before()` is null, triggering the assert.
    if (neighbor_addr == nullptr)
        return nullptr;

    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
    RadixTreeLeafElm * elm = rtree.leafElmLookup(
        tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(neighbor_addr), /* dependent */ false, /* init_missing */ false);
    if (elm == nullptr)
        return nullptr;

    RadixTreeContents neighbor_contents = RadixTree::leafElmRead(tsdn, elm, /* dependent */ false);
    if (!extentCanAcquireNeighbor(edata, neighbor_contents, pai, expected_state, forward, expanding))
        return nullptr;

    /// From this point, the neighbor extent can be safely acquired.
    Extent * neighbor = neighbor_contents.edata;
    JE_ASSERT(neighbor->state() == expected_state);
    updateEdataState(tsdn, neighbor, extent_state_merging);
    if (expanding)
        extentAssertCanExpand(edata, neighbor);
    else
        extentAssertCanCoalesce(edata, neighbor);

    return neighbor;
}

Extent * ExtentMap::tryAcquireEdataNeighbor(ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state, bool forward)
{
    return tryAcquireEdataNeighborImpl(tsdn, edata, pai, expected_state, forward, /* expanding */ false);
}

Extent * ExtentMap::tryAcquireEdataNeighborExpand(ThreadState * tsdn, Extent * edata, ExtentPai pai, ExtentState expected_state)
{
    /// Try expanding forward.
    return tryAcquireEdataNeighborImpl(tsdn, edata, pai, expected_state, /* forward */ true, /* expanding */ true);
}

void ExtentMap::releaseEdata(ThreadState * tsdn, Extent * edata, ExtentState new_state)
{
    JE_ASSERT(edataInTransition(tsdn, edata));
    JE_ASSERT(edataIsAcquired(tsdn, edata));

    updateEdataState(tsdn, edata, new_state);
}

bool ExtentMap::rtreeLeafElmsLookup(
    ThreadState * tsdn,
    RadixTreeContext * rtree_ctx,
    const Extent * edata,
    bool dependent,
    bool init_missing,
    RadixTreeLeafElm ** r_elm_a,
    RadixTreeLeafElm ** r_elm_b)
{
    *r_elm_a = rtree.leafElmLookup(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()), dependent, init_missing);
    if (!dependent && *r_elm_a == nullptr)
        return true;
    JE_ASSERT(*r_elm_a != nullptr);

    *r_elm_b = rtree.leafElmLookup(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->last()), dependent, init_missing);
    if (!dependent && *r_elm_b == nullptr)
        return true;
    JE_ASSERT(*r_elm_b != nullptr);

    return false;
}

void ExtentMap::rtreeWriteAcquired(
    ThreadState * tsdn, RadixTreeLeafElm * elm_a, RadixTreeLeafElm * elm_b, Extent * edata, szind_t szind, bool slab)
{
    RadixTreeContents contents;
    contents.edata = edata;
    contents.metadata.szind = szind;
    contents.metadata.slab = slab;
    contents.metadata.is_head = (edata == nullptr) ? false : edata->isHead();
    contents.metadata.state = (edata == nullptr) ? ExtentState(0) : edata->state();
    RadixTree::leafElmWrite(tsdn, elm_a, contents);
    if (elm_b != nullptr)
        RadixTree::leafElmWrite(tsdn, elm_b, contents);
}

bool ExtentMap::registerBoundary(ThreadState * tsdn, Extent * edata, szind_t szind, bool slab)
{
    JE_ASSERT(edata->state() == extent_state_active);
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    RadixTreeLeafElm * elm_a;
    RadixTreeLeafElm * elm_b;
    bool err = rtreeLeafElmsLookup(tsdn, rtree_ctx, edata, false, true, &elm_a, &elm_b);
    if (err)
        return true;
    JE_ASSERT(RadixTree::leafElmRead(tsdn, elm_a, /* dependent */ false).edata == nullptr);
    JE_ASSERT(RadixTree::leafElmRead(tsdn, elm_b, /* dependent */ false).edata == nullptr);
    rtreeWriteAcquired(tsdn, elm_a, elm_b, edata, szind, slab);
    return false;
}

void ExtentMap::registerInterior(ThreadState * tsdn, Extent * edata, szind_t szind)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    JE_ASSERT(edata->slab());
    JE_ASSERT(edata->state() == extent_state_active);

    if constexpr (config::debug)
    {
        /// Making sure the boundary is registered already.
        RadixTreeLeafElm * elm_a;
        RadixTreeLeafElm * elm_b;
        [[maybe_unused]] bool err
            = rtreeLeafElmsLookup(tsdn, rtree_ctx, edata, /* dependent */ true, /* init_missing */ false, &elm_a, &elm_b);
        JE_ASSERT(!err);
        [[maybe_unused]] RadixTreeContents contents_a = RadixTree::leafElmRead(tsdn, elm_a, /* dependent */ true);
        [[maybe_unused]] RadixTreeContents contents_b = RadixTree::leafElmRead(tsdn, elm_b, /* dependent */ true);
        JE_ASSERT(contents_a.edata == edata && contents_b.edata == edata);
        JE_ASSERT(contents_a.metadata.slab && contents_b.metadata.slab);
    }

    RadixTreeContents contents;
    contents.edata = edata;
    contents.metadata.szind = szind;
    contents.metadata.slab = true;
    contents.metadata.state = extent_state_active;
    contents.metadata.is_head = false; /// Not allowed to access.

    JE_ASSERT(edata->size() > (2 << LG_PAGE));
    rtree.writeRange(
        tsdn,
        rtree_ctx,
        reinterpret_cast<uintptr_t>(edata->base()) + PAGE,
        reinterpret_cast<uintptr_t>(edata->last()) - PAGE,
        contents);
}

void ExtentMap::deregisterBoundary(ThreadState * tsdn, Extent * edata)
{
    /// The extent must be either in an acquired state, or protected by state based locks (witness is not
    /// implemented, so there is nothing to check in the latter case).

    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
    RadixTreeLeafElm * elm_a;
    RadixTreeLeafElm * elm_b;

    rtreeLeafElmsLookup(tsdn, rtree_ctx, edata, true, false, &elm_a, &elm_b);
    rtreeWriteAcquired(tsdn, elm_a, elm_b, nullptr, SC_NSIZES, false);
}

void ExtentMap::deregisterInterior(ThreadState * tsdn, Extent * edata)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    JE_ASSERT(edata->slab());
    if (edata->size() > (2 << LG_PAGE))
    {
        rtree.clearRange(
            tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()) + PAGE, reinterpret_cast<uintptr_t>(edata->last()) - PAGE);
    }
}

void ExtentMap::remap(ThreadState * tsdn, Extent * edata, szind_t szind, bool slab)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    if (szind != SC_NSIZES)
    {
        RadixTreeContents contents;
        contents.edata = edata;
        contents.metadata.szind = szind;
        contents.metadata.slab = slab;
        contents.metadata.is_head = edata->isHead();
        contents.metadata.state = edata->state();

        rtree.write(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->addr()), contents);
        /// Recall that this is called only for active->inactive and inactive->active transitions (since only active
        /// extents have meaningful values for szind and slab). Active, non-slab extents only need to handle lookups
        /// at their head (on deallocation), so we don't bother filling in the end boundary.
        ///
        /// For slab extents, we do the end-mapping change. This still leaves the interior unmodified; a
        /// `registerInterior` call is coming in those cases, though.
        if (slab && edata->size() > PAGE)
        {
            uintptr_t key = reinterpret_cast<uintptr_t>(edata->past()) - uintptr_t(PAGE);
            rtree.write(tsdn, rtree_ctx, key, contents);
        }
    }
}

bool ExtentMap::splitPrepare(
    ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * edata, size_t size_a, Extent * trail, size_t /*size_b*/)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    /// We use incorrect constants for things like arena ind, zero, ranged, and commit state, and head status. This
    /// is a fake extent, used to facilitate a lookup.
    Extent lead{};
    lead.init(0U, edata->addr(), size_a, false, 0, 0, extent_state_active, false, false, EXTENT_PAI_PAC, EXTENT_NOT_HEAD);

    rtreeLeafElmsLookup(tsdn, rtree_ctx, &lead, false, true, &prepare->lead_elm_a, &prepare->lead_elm_b);
    rtreeLeafElmsLookup(tsdn, rtree_ctx, trail, false, true, &prepare->trail_elm_a, &prepare->trail_elm_b);

    if (prepare->lead_elm_a == nullptr || prepare->lead_elm_b == nullptr || prepare->trail_elm_a == nullptr
        || prepare->trail_elm_b == nullptr)
        return true;
    return false;
}

void ExtentMap::splitCommit(
    ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, size_t /*size_a*/, Extent * trail, size_t /*size_b*/)
{
    /// We should think about not writing to the lead leaf element. We can get into situations where a racing
    /// realloc-like call can disagree with a size lookup request. It's fine to declare that these situations are race
    /// bugs, but there's an argument to be made that for things like xallocx, a size lookup call should return either
    /// the old size or the new size, but not anything else.
    rtreeWriteAcquired(tsdn, prepare->lead_elm_a, prepare->lead_elm_b, lead, SC_NSIZES, /* slab */ false);
    rtreeWriteAcquired(tsdn, prepare->trail_elm_a, prepare->trail_elm_b, trail, SC_NSIZES, /* slab */ false);
}

void ExtentMap::mergePrepare(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, Extent * trail)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);
    rtreeLeafElmsLookup(tsdn, rtree_ctx, lead, true, false, &prepare->lead_elm_a, &prepare->lead_elm_b);
    rtreeLeafElmsLookup(tsdn, rtree_ctx, trail, true, false, &prepare->trail_elm_a, &prepare->trail_elm_b);
}

void ExtentMap::mergeCommit(ThreadState * tsdn, ExtentMapPrepare * prepare, Extent * lead, Extent * /*trail*/)
{
    if (prepare->lead_elm_b != nullptr)
        RadixTree::leafElmWrite(tsdn, prepare->lead_elm_b, rtree_contents_cleared);

    RadixTreeLeafElm * merged_b;
    if (prepare->trail_elm_b != nullptr)
    {
        RadixTree::leafElmWrite(tsdn, prepare->trail_elm_a, rtree_contents_cleared);
        merged_b = prepare->trail_elm_b;
    }
    else
    {
        merged_b = prepare->trail_elm_a;
    }

    rtreeWriteAcquired(tsdn, prepare->lead_elm_a, merged_b, lead, SC_NSIZES, false);
}

void ExtentMap::doAssertMapped(ThreadState * tsdn, Extent * edata)
{
    RadixTreeContext rtree_ctx_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * rtree_ctx = ThreadState::tsdnRtreeCtx(tsdn, &rtree_ctx_fallback);

    [[maybe_unused]] RadixTreeContents contents = rtree.read(tsdn, rtree_ctx, reinterpret_cast<uintptr_t>(edata->base()));
    JE_ASSERT(contents.edata == edata);
    JE_ASSERT(contents.metadata.is_head == edata->isHead());
    JE_ASSERT(contents.metadata.state == edata->state());
}

void ExtentMap::doAssertNotMapped(ThreadState * tsdn, Extent * edata)
{
    FullAllocContext context1{};
    fullAllocCtxTryLookup(tsdn, edata->base(), &context1);
    JE_ASSERT(context1.edata == nullptr);

    FullAllocContext context2{};
    fullAllocCtxTryLookup(tsdn, edata->last(), &context2);
    JE_ASSERT(context2.edata == nullptr);
}

}
