#include <allocator/ExtentMap.h>

namespace jemalloc
{

constinit ExtentMap arena_extent_map_global;

void ExtentMap::updateExtentState(ThreadState * thread_state, Extent * extent, ExtentState state)
{
    extent->setState(state);

    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
    RadixTreeLeafElement * element1 = radix_tree.leafElementLookup(
        thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->base()), /* dependent */ true, /* init_missing */ false);
    ALLOCATOR_ASSERT(element1 != nullptr);
    RadixTreeLeafElement * element2 = extent->size() == PAGE ? nullptr
                                                             : radix_tree.leafElementLookup(
                                                                   thread_state,
                                                                   radix_tree_context,
                                                                   reinterpret_cast<uintptr_t>(extent->last()),
                                                                   /* dependent */ true,
                                                                   /* init_missing */ false);

    RadixTree::leafElementStateUpdate(thread_state, element1, element2, state);

    assertMapped(thread_state, extent);
}

Extent * ExtentMap::tryAcquireExtentNeighborImpl(
    ThreadState * thread_state,
    Extent * extent,
    ExtentAllocatorKind allocator_kind,
    ExtentState expected_state,
    bool forward,
    bool expanding)
{
    ALLOCATOR_ASSERT(!extent->guarded());
    ALLOCATOR_ASSERT(!expanding || forward);
    ALLOCATOR_ASSERT(!extentStateInTransition(expected_state));
    ALLOCATOR_ASSERT(
        expected_state == extent_state_dirty || expected_state == extent_state_muzzy || expected_state == extent_state_retained);

    void * neighbor_addr = forward ? extent->past() : extent->before();
    /// This is subtle; the rtree code asserts that its input pointer is non-null, and this is a useful thing to check.
    /// But it's possible that the extent corresponds to an address of `(void *)PAGE` (in practice, this has only been
    /// observed on FreeBSD when address-space randomization is on, but it could in principle happen anywhere). In this
    /// case, `before()` is null, triggering the assert.
    if (neighbor_addr == nullptr)
        return nullptr;

    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
    RadixTreeLeafElement * element = radix_tree.leafElementLookup(
        thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(neighbor_addr), /* dependent */ false, /* init_missing */ false);
    if (element == nullptr)
        return nullptr;

    RadixTreeContents neighbor_contents = RadixTree::leafElementRead(thread_state, element, /* dependent */ false);
    if (!extentCanAcquireNeighbor(extent, neighbor_contents, allocator_kind, expected_state, forward, expanding))
        return nullptr;

    /// From this point, the neighbor extent can be safely acquired.
    Extent * neighbor = neighbor_contents.extent;
    ALLOCATOR_ASSERT(neighbor->state() == expected_state);
    updateExtentState(thread_state, neighbor, extent_state_merging);
    if (expanding)
        extentAssertCanExpand(extent, neighbor);
    else
        extentAssertCanCoalesce(extent, neighbor);

    return neighbor;
}

Extent * ExtentMap::tryAcquireExtentNeighbor(
    ThreadState * thread_state, Extent * extent, ExtentAllocatorKind allocator_kind, ExtentState expected_state, bool forward)
{
    return tryAcquireExtentNeighborImpl(thread_state, extent, allocator_kind, expected_state, forward, /* expanding */ false);
}

Extent * ExtentMap::tryAcquireExtentNeighborExpand(
    ThreadState * thread_state, Extent * extent, ExtentAllocatorKind allocator_kind, ExtentState expected_state)
{
    /// Try expanding forward.
    return tryAcquireExtentNeighborImpl(thread_state, extent, allocator_kind, expected_state, /* forward */ true, /* expanding */ true);
}

void ExtentMap::releaseExtent(ThreadState * thread_state, Extent * extent, ExtentState new_state)
{
    ALLOCATOR_ASSERT(extentInTransition(thread_state, extent));
    ALLOCATOR_ASSERT(extentIsAcquired(thread_state, extent));

    updateExtentState(thread_state, extent, new_state);
}

bool ExtentMap::radixTreeLeafElementsLookup(
    ThreadState * thread_state,
    RadixTreeContext * radix_tree_context,
    const Extent * extent,
    bool dependent,
    bool init_missing,
    RadixTreeLeafElement ** r_element_a,
    RadixTreeLeafElement ** r_element_b)
{
    *r_element_a = radix_tree.leafElementLookup(
        thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->base()), dependent, init_missing);
    if (!dependent && *r_element_a == nullptr)
        return true;
    ALLOCATOR_ASSERT(*r_element_a != nullptr);

    *r_element_b = radix_tree.leafElementLookup(
        thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->last()), dependent, init_missing);
    if (!dependent && *r_element_b == nullptr)
        return true;
    ALLOCATOR_ASSERT(*r_element_b != nullptr);

    return false;
}

void ExtentMap::radixTreeWriteAcquired(
    ThreadState * thread_state,
    RadixTreeLeafElement * element_a,
    RadixTreeLeafElement * element_b,
    Extent * extent,
    SizeClassIdx size_class_idx,
    bool slab)
{
    RadixTreeContents contents;
    contents.extent = extent;
    contents.metadata.size_class_idx = size_class_idx;
    contents.metadata.slab = slab;
    contents.metadata.is_head = (extent == nullptr) ? false : extent->isHead();
    contents.metadata.state = (extent == nullptr) ? ExtentState(0) : extent->state();
    RadixTree::leafElementWrite(thread_state, element_a, contents);
    if (element_b != nullptr)
        RadixTree::leafElementWrite(thread_state, element_b, contents);
}

bool ExtentMap::registerBoundary(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx, bool slab)
{
    ALLOCATOR_ASSERT(extent->state() == extent_state_active);
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    RadixTreeLeafElement * element_a;
    RadixTreeLeafElement * element_b;
    bool error = radixTreeLeafElementsLookup(thread_state, radix_tree_context, extent, false, true, &element_a, &element_b);
    if (error)
        return true;
    ALLOCATOR_ASSERT(RadixTree::leafElementRead(thread_state, element_a, /* dependent */ false).extent == nullptr);
    ALLOCATOR_ASSERT(RadixTree::leafElementRead(thread_state, element_b, /* dependent */ false).extent == nullptr);
    radixTreeWriteAcquired(thread_state, element_a, element_b, extent, size_class_idx, slab);
    return false;
}

void ExtentMap::registerInterior(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    ALLOCATOR_ASSERT(extent->slab());
    ALLOCATOR_ASSERT(extent->state() == extent_state_active);

    if constexpr (config::debug)
    {
        /// Making sure the boundary is registered already.
        RadixTreeLeafElement * element_a;
        RadixTreeLeafElement * element_b;
        [[maybe_unused]] bool error = radixTreeLeafElementsLookup(
            thread_state, radix_tree_context, extent, /* dependent */ true, /* init_missing */ false, &element_a, &element_b);
        ALLOCATOR_ASSERT(!error);
        [[maybe_unused]] RadixTreeContents contents_a = RadixTree::leafElementRead(thread_state, element_a, /* dependent */ true);
        [[maybe_unused]] RadixTreeContents contents_b = RadixTree::leafElementRead(thread_state, element_b, /* dependent */ true);
        ALLOCATOR_ASSERT(contents_a.extent == extent && contents_b.extent == extent);
        ALLOCATOR_ASSERT(contents_a.metadata.slab && contents_b.metadata.slab);
    }

    RadixTreeContents contents;
    contents.extent = extent;
    contents.metadata.size_class_idx = size_class_idx;
    contents.metadata.slab = true;
    contents.metadata.state = extent_state_active;
    contents.metadata.is_head = false; /// Not allowed to access.

    ALLOCATOR_ASSERT(extent->size() > (2 << LOG2_PAGE));
    radix_tree.writeRange(
        thread_state,
        radix_tree_context,
        reinterpret_cast<uintptr_t>(extent->base()) + PAGE,
        reinterpret_cast<uintptr_t>(extent->last()) - PAGE,
        contents);
}

void ExtentMap::deregisterBoundary(ThreadState * thread_state, Extent * extent)
{
    /// The extent must be either in an acquired state, or protected by state based locks (witness is not
    /// implemented, so there is nothing to check in the latter case).

    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
    RadixTreeLeafElement * element_a;
    RadixTreeLeafElement * element_b;

    radixTreeLeafElementsLookup(thread_state, radix_tree_context, extent, true, false, &element_a, &element_b);
    radixTreeWriteAcquired(thread_state, element_a, element_b, nullptr, SIZE_CLASS_NUM_SIZES, false);
}

void ExtentMap::deregisterInterior(ThreadState * thread_state, Extent * extent)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    ALLOCATOR_ASSERT(extent->slab());
    if (extent->size() > (2 << LOG2_PAGE))
    {
        radix_tree.clearRange(
            thread_state,
            radix_tree_context,
            reinterpret_cast<uintptr_t>(extent->base()) + PAGE,
            reinterpret_cast<uintptr_t>(extent->last()) - PAGE);
    }
}

void ExtentMap::remap(ThreadState * thread_state, Extent * extent, SizeClassIdx size_class_idx, bool slab)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    if (size_class_idx != SIZE_CLASS_NUM_SIZES)
    {
        RadixTreeContents contents;
        contents.extent = extent;
        contents.metadata.size_class_idx = size_class_idx;
        contents.metadata.slab = slab;
        contents.metadata.is_head = extent->isHead();
        contents.metadata.state = extent->state();

        radix_tree.write(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->addr()), contents);
        /// Recall that this is called only for active->inactive and inactive->active transitions (since only active
        /// extents have meaningful values for szind and slab). Active, non-slab extents only need to handle lookups
        /// at their head (on deallocation), so we don't bother filling in the end boundary.
        ///
        /// For slab extents, we do the end-mapping change. This still leaves the interior unmodified; a
        /// `registerInterior` call is coming in those cases, though.
        if (slab && extent->size() > PAGE)
        {
            uintptr_t key = reinterpret_cast<uintptr_t>(extent->past()) - uintptr_t(PAGE);
            radix_tree.write(thread_state, radix_tree_context, key, contents);
        }
    }
}

bool ExtentMap::splitPrepare(
    ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * extent, size_t size_a, Extent * trail, size_t /*size_b*/)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    /// We use incorrect constants for things like arena ind, zero, ranged, and commit state, and head status. This
    /// is a fake extent, used to facilitate a lookup.
    Extent lead{};
    lead.init(0U, extent->addr(), size_a, false, 0, 0, extent_state_active, false, false, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, EXTENT_NOT_HEAD);

    radixTreeLeafElementsLookup(thread_state, radix_tree_context, &lead, false, true, &prepare->lead_element_a, &prepare->lead_element_b);
    radixTreeLeafElementsLookup(thread_state, radix_tree_context, trail, false, true, &prepare->trail_element_a, &prepare->trail_element_b);

    if (prepare->lead_element_a == nullptr || prepare->lead_element_b == nullptr || prepare->trail_element_a == nullptr
        || prepare->trail_element_b == nullptr)
        return true;
    return false;
}

void ExtentMap::splitCommit(
    ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, size_t /*size_a*/, Extent * trail, size_t /*size_b*/)
{
    /// We should think about not writing to the lead leaf element. We can get into situations where a racing
    /// realloc-like call can disagree with a size lookup request. It's fine to declare that these situations are race
    /// bugs, but there's an argument to be made that for things like xallocx, a size lookup call should return either
    /// the old size or the new size, but not anything else.
    radixTreeWriteAcquired(thread_state, prepare->lead_element_a, prepare->lead_element_b, lead, SIZE_CLASS_NUM_SIZES, /* slab */ false);
    radixTreeWriteAcquired(thread_state, prepare->trail_element_a, prepare->trail_element_b, trail, SIZE_CLASS_NUM_SIZES, /* slab */ false);
}

void ExtentMap::mergePrepare(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, Extent * trail)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);
    radixTreeLeafElementsLookup(thread_state, radix_tree_context, lead, true, false, &prepare->lead_element_a, &prepare->lead_element_b);
    radixTreeLeafElementsLookup(thread_state, radix_tree_context, trail, true, false, &prepare->trail_element_a, &prepare->trail_element_b);
}

void ExtentMap::mergeCommit(ThreadState * thread_state, ExtentMapPrepare * prepare, Extent * lead, Extent * /*trail*/)
{
    if (prepare->lead_element_b != nullptr)
        RadixTree::leafElementWrite(thread_state, prepare->lead_element_b, radix_tree_contents_cleared);

    RadixTreeLeafElement * merged_b;
    if (prepare->trail_element_b != nullptr)
    {
        RadixTree::leafElementWrite(thread_state, prepare->trail_element_a, radix_tree_contents_cleared);
        merged_b = prepare->trail_element_b;
    }
    else
    {
        merged_b = prepare->trail_element_a;
    }

    radixTreeWriteAcquired(thread_state, prepare->lead_element_a, merged_b, lead, SIZE_CLASS_NUM_SIZES, false);
}

void ExtentMap::doAssertMapped(ThreadState * thread_state, Extent * extent)
{
    RadixTreeContext radix_tree_context_fallback{RadixTreeContext::NoInit{}};
    RadixTreeContext * radix_tree_context = ThreadState::threadStateRadixTreeContext(thread_state, &radix_tree_context_fallback);

    [[maybe_unused]] RadixTreeContents contents
        = radix_tree.read(thread_state, radix_tree_context, reinterpret_cast<uintptr_t>(extent->base()));
    ALLOCATOR_ASSERT(contents.extent == extent);
    ALLOCATOR_ASSERT(contents.metadata.is_head == extent->isHead());
    ALLOCATOR_ASSERT(contents.metadata.state == extent->state());
}

void ExtentMap::doAssertNotMapped(ThreadState * thread_state, Extent * extent)
{
    FullAllocContext context1{};
    fullAllocContextTryLookup(thread_state, extent->base(), &context1);
    ALLOCATOR_ASSERT(context1.extent == nullptr);

    FullAllocContext context2{};
    fullAllocContextTryLookup(thread_state, extent->last(), &context2);
    ALLOCATOR_ASSERT(context2.extent == nullptr);
}

}
