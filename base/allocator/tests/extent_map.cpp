/// Tests of `ExtentMap` (`emap.c`): boundary/interior registration and lookups, remap, split and merge, neighbor
/// acquisition rules, the fast lookup through the thread's rtree cache, and the batch lookup. The map is the global
/// `arena_extent_map_global` on `b0`, initialized as at boot; extent addresses are fake (never dereferenced).

#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Pages.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>

using namespace jemalloc;

namespace
{

constinit ThreadState test_thread_state;

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    REQUIRE(!pages::boot());
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_extent_map_global.init(base0Get(), true));
    booted = true;
}

Extent * newExtent()
{
    void * p = std::aligned_alloc(EXTENT_ALIGNMENT, sizeof(Extent));
    std::memset(p, 0, sizeof(Extent));
    return static_cast<Extent *>(p);
}

/// A fresh region of fake addresses for each test.
std::byte * region(unsigned i)
{
    return reinterpret_cast<std::byte *>((uintptr_t(1) << 40) + uintptr_t(i) * (uintptr_t(1) << 34));
}

Extent * makeExtent(void * addr, size_t size, ExtentState state, bool slab = false, bool is_head = false, bool committed = true)
{
    Extent * e = newExtent();
    e->init(
        0,
        addr,
        size,
        slab,
        SIZE_CLASS_NUM_SIZES,
        1,
        state,
        false,
        committed,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        is_head ? EXTENT_IS_HEAD : EXTENT_NOT_HEAD);
    return e;
}

/// Registers with the state set to active (as `registerBoundary` requires), then sets the actual state.
void registerExtent(
    ExtentMap & extent_map, Extent * e, ExtentState state, SizeClassIdx size_class_idx = SIZE_CLASS_NUM_SIZES, bool slab = false)
{
    e->setState(extent_state_active);
    REQUIRE(!extent_map.registerBoundary(nullptr, e, size_class_idx, slab));
    if (state != extent_state_active)
        extent_map.updateExtentState(nullptr, e, state);
}

RadixTreeContents readContents(ExtentMap & extent_map, const void * ptr)
{
    RadixTreeContext fallback;
    RadixTreeContext * context = ThreadState::threadStateRadixTreeContext(nullptr, &fallback);
    return extent_map.radix_tree.read(nullptr, context, reinterpret_cast<uintptr_t>(ptr));
}

}

TEST(ExtentMap, ThreadStateAccessors)
{
    static_assert(sizeof(RadixTreeContext) == 384);
    RadixTreeContext fallback;
    fallback.cache[0].leaf_key = 77;
    CHECK(ThreadState::threadStateRadixTreeContext(nullptr, &fallback) == &fallback);
    CHECK_EQ(fallback.cache[0].leaf_key, RADIX_TREE_LEAF_KEY_INVALID);
    CHECK(ThreadState::threadStateRadixTreeContext(&test_thread_state, &fallback) == &test_thread_state.radix_tree_context);
    CHECK(test_thread_state.radixTreeContext() == &test_thread_state.radix_tree_context);
    CHECK_EQ(test_thread_state.stateGet(), uint8_t(thread_state_uninitialized));
    CHECK_EQ(test_thread_state.arena_decay_ticker.read(), 1000);
    CHECK_EQ(test_thread_state.bin_shards.bin_shard[0], uint8_t(UINT8_MAX));
    CHECK_EQ(test_thread_state.bin_shards.bin_shard[1], 0u);
    CHECK_EQ(test_thread_state.radix_tree_context.l2_cache[7].leaf_key, RADIX_TREE_LEAF_KEY_INVALID);
    test_thread_state.prngState() = 5;
    CHECK_EQ(test_thread_state.prng_state, 5u);
    test_thread_state.reentrancyLevel() = 0;
    CHECK_EQ(test_thread_state.reentrancy_level, 0);
}

TEST(ExtentMap, RegisterLookupDeregister)
{
    bootOnce();
    ExtentMap & extent_map = arena_extent_map_global;
    std::byte * base = region(0);

    /// One-page extent: both boundaries are the same element.
    Extent * one = makeExtent(base, PAGE, extent_state_active, false, true);
    registerExtent(extent_map, one, extent_state_active);
    CHECK_EQ(extent_map.extentLookup(nullptr, base), one);
    RadixTreeContents c = readContents(extent_map, base);
    CHECK_EQ(c.metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
    CHECK(!c.metadata.slab);
    CHECK(c.metadata.is_head);
    CHECK_EQ(c.metadata.state, extent_state_active);

    /// Large extent: first and last pages; interior pages are not registered.
    std::byte * large_addr = base + 4 * PAGE;
    size_t usable_size = SIZE_CLASS_LARGE_MIN_CLASS + 2 * PAGE;
    Extent * large = makeExtent(large_addr, usable_size + large_pad, extent_state_active);
    registerExtent(extent_map, large, extent_state_active);
    CHECK_EQ(extent_map.extentLookup(nullptr, large_addr), large);
    CHECK_EQ(extent_map.extentLookup(nullptr, large->last()), large);
    CHECK(extent_map.extentLookup(nullptr, large_addr + PAGE) == nullptr);

    /// Remap sets szind/slab at the head only (for non-slabs).
    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);
    large->setSizeClassIdx(size_class_idx);
    extent_map.remap(nullptr, large, size_class_idx, false);
    AllocContext alloc_context;
    extent_map.allocContextLookup(nullptr, large_addr, &alloc_context);
    CHECK_EQ(alloc_context.size_class_idx, size_class_idx);
    CHECK(!alloc_context.slab);
    CHECK_EQ(alloc_context.usable_size, usable_size);
    CHECK_EQ(alloc_context.usableSizeGet(), usable_size);
    CHECK_EQ(readContents(extent_map, large->last()).metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
    /// Remap with SIZE_CLASS_NUM_SIZES is a no-op.
    extent_map.remap(nullptr, large, SIZE_CLASS_NUM_SIZES, false);
    CHECK_EQ(readContents(extent_map, large_addr).metadata.size_class_idx, size_class_idx);

    FullAllocContext full;
    extent_map.fullAllocContextLookup(nullptr, large_addr, &full);
    CHECK_EQ(full.extent, large);
    CHECK_EQ(full.size_class_idx, size_class_idx);
    CHECK(!full.slab);

    /// Not mapped, but in an existing leaf: found with null edata. In a leaf that doesn't exist: not present.
    FullAllocContext missing{};
    CHECK(!extent_map.fullAllocContextTryLookup(nullptr, base + 100 * PAGE, &missing));
    CHECK(missing.extent == nullptr);
    CHECK(extent_map.fullAllocContextTryLookup(nullptr, region(1000), &missing));
    AllocContext missing_context;
    extent_map.allocContextLookup(nullptr, base + 100 * PAGE, &missing_context);
    CHECK_EQ(missing_context.size_class_idx, 0u);
    CHECK_EQ(missing_context.usable_size, 0u);

    extent_map.deregisterBoundary(nullptr, large);
    extent_map.assertNotMapped(nullptr, large);
    c = readContents(extent_map, large_addr);
    CHECK(c.extent == nullptr);
    CHECK_EQ(c.metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
    extent_map.deregisterBoundary(nullptr, one);
    CHECK(extent_map.extentLookup(nullptr, base) == nullptr);
    std::free(one);
    std::free(large);
}

TEST(ExtentMap, Slab)
{
    bootOnce();
    ExtentMap & extent_map = arena_extent_map_global;
    std::byte * base = region(1);
    SizeClassIdx bin_idx = 3;

    Extent * slab = makeExtent(base, 8 * PAGE, extent_state_active, true);
    slab->setSizeClassIdx(bin_idx);
    registerExtent(extent_map, slab, extent_state_active, bin_idx, true);
    extent_map.registerInterior(nullptr, slab, bin_idx);
    for (size_t i = 0; i < 8; ++i)
    {
        RadixTreeContents c = readContents(extent_map, base + i * PAGE + 16);
        CHECK_EQ(c.extent, slab);
        CHECK_EQ(c.metadata.size_class_idx, bin_idx);
        CHECK(c.metadata.slab);
        CHECK_EQ(c.metadata.state, extent_state_active);
    }
    AllocContext alloc_context;
    extent_map.allocContextLookup(nullptr, base + 5 * PAGE + 48, &alloc_context);
    CHECK(alloc_context.slab);
    CHECK_EQ(alloc_context.size_class_idx, bin_idx);
    CHECK_EQ(alloc_context.usable_size, size_classes::indexToSize(bin_idx));

    /// Remap of a slab also writes the last page.
    extent_map.remap(nullptr, slab, bin_idx + 1, true);
    CHECK_EQ(readContents(extent_map, slab->last()).metadata.size_class_idx, bin_idx + 1);
    CHECK_EQ(readContents(extent_map, base + PAGE).metadata.size_class_idx, bin_idx);

    extent_map.deregisterInterior(nullptr, slab);
    for (size_t i = 1; i < 7; ++i)
    {
        RadixTreeContents c = readContents(extent_map, base + i * PAGE);
        CHECK(c.extent == nullptr);
        CHECK_EQ(c.metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
    }
    CHECK_EQ(extent_map.extentLookup(nullptr, base), slab);
    extent_map.deregisterBoundary(nullptr, slab);
    extent_map.assertNotMapped(nullptr, slab);

    /// Small slabs (<= 2 pages) have no interior.
    Extent * small = makeExtent(base + 16 * PAGE, 2 * PAGE, extent_state_active, true);
    registerExtent(extent_map, small, extent_state_active, bin_idx, true);
    extent_map.deregisterInterior(nullptr, small);
    CHECK_EQ(extent_map.extentLookup(nullptr, small->last()), small);
    extent_map.deregisterBoundary(nullptr, small);
    std::free(slab);
    std::free(small);
}

TEST(ExtentMap, SplitMerge)
{
    bootOnce();
    ExtentMap & extent_map = arena_extent_map_global;
    std::byte * base = region(2);

    Extent * e = makeExtent(base, 8 * PAGE, extent_state_active);
    registerExtent(extent_map, e, extent_state_active);

    /// Split into 3 + 5 pages.
    Extent * trail = makeExtent(base + 3 * PAGE, 5 * PAGE, extent_state_active);
    ExtentMapPrepare prepare;
    CHECK(!extent_map.splitPrepare(nullptr, &prepare, e, 3 * PAGE, trail, 5 * PAGE));
    e->setSize(3 * PAGE);
    extent_map.splitCommit(nullptr, &prepare, e, 3 * PAGE, trail, 5 * PAGE);
    CHECK_EQ(extent_map.extentLookup(nullptr, base), e);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 2 * PAGE), e);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 7 * PAGE), trail);
    CHECK(extent_map.extentLookup(nullptr, base + 5 * PAGE) == nullptr);
    CHECK_EQ(readContents(extent_map, base).metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);

    /// Split of a one-page lead: both lead elements are the same.
    Extent * trail2 = makeExtent(base + 4 * PAGE, 4 * PAGE, extent_state_active);
    CHECK(!extent_map.splitPrepare(nullptr, &prepare, trail, PAGE, trail2, 4 * PAGE));
    CHECK(prepare.lead_element_a == prepare.lead_element_b);
    trail->setSize(PAGE);
    extent_map.splitCommit(nullptr, &prepare, trail, PAGE, trail2, 4 * PAGE);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 4 * PAGE), trail2);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 7 * PAGE), trail2);

    /// Merge trail (1 page) + trail2 (4 pages): the inner boundaries are cleared.
    extent_map.mergePrepare(nullptr, &prepare, trail, trail2);
    CHECK(prepare.lead_element_a == prepare.lead_element_b);
    extent_map.mergeCommit(nullptr, &prepare, trail, trail2);
    trail->setSize(5 * PAGE);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 7 * PAGE), trail);
    CHECK(extent_map.extentLookup(nullptr, base + 4 * PAGE) == nullptr);

    /// Merge e (3 pages) + trail (5 pages).
    extent_map.mergePrepare(nullptr, &prepare, e, trail);
    extent_map.mergeCommit(nullptr, &prepare, e, trail);
    e->setSize(8 * PAGE);
    CHECK_EQ(extent_map.extentLookup(nullptr, base), e);
    CHECK_EQ(extent_map.extentLookup(nullptr, base + 7 * PAGE), e);
    for (size_t i = 1; i < 7; ++i)
        CHECK(extent_map.extentLookup(nullptr, base + i * PAGE) == nullptr);
    RadixTreeContents c = readContents(extent_map, base + 2 * PAGE);
    CHECK_EQ(c.metadata.size_class_idx, SIZE_CLASS_NUM_SIZES);
    CHECK_EQ(c.metadata.state, extent_state_active);

    extent_map.deregisterBoundary(nullptr, e);
    std::free(e);
    std::free(trail);
    std::free(trail2);
}

TEST(ExtentMap, NeighborAcquisition)
{
    bootOnce();
    ExtentMap & extent_map = arena_extent_map_global;
    std::byte * base = region(3);

    /// [prev: dirty][e: active][next: dirty][far: retained, head]
    Extent * prev = makeExtent(base, 2 * PAGE, extent_state_dirty);
    Extent * e = makeExtent(base + 2 * PAGE, 2 * PAGE, extent_state_active);
    Extent * next = makeExtent(base + 4 * PAGE, 3 * PAGE, extent_state_dirty);
    registerExtent(extent_map, prev, extent_state_dirty);
    registerExtent(extent_map, e, extent_state_active);
    registerExtent(extent_map, next, extent_state_dirty);
    CHECK_EQ(readContents(extent_map, next->last()).metadata.state, extent_state_dirty);

    /// Wrong expected state.
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_muzzy, true) == nullptr);
    /// Forward.
    Extent * got = extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true);
    CHECK_EQ(got, next);
    CHECK_EQ(next->state(), extent_state_merging);
    CHECK_EQ(readContents(extent_map, next->addr()).metadata.state, extent_state_merging);
    CHECK_EQ(readContents(extent_map, next->last()).metadata.state, extent_state_merging);
    /// Already acquired: the state no longer matches.
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true) == nullptr);
    extent_map.releaseExtent(nullptr, next, extent_state_dirty);
    CHECK_EQ(next->state(), extent_state_dirty);
    CHECK_EQ(readContents(extent_map, next->last()).metadata.state, extent_state_dirty);

    /// Backward.
    got = extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, false);
    CHECK_EQ(got, prev);
    extent_map.releaseExtent(nullptr, prev, extent_state_dirty);

    /// Head states: no forward merge into a head neighbor; no backward merge when the extent itself is a head.
    extent_map.deregisterBoundary(nullptr, next);
    next->setIsHead(true);
    registerExtent(extent_map, next, extent_state_dirty);
    CHECK(readContents(extent_map, next->addr()).metadata.is_head);
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true) == nullptr);
    CHECK(extent_map.tryAcquireExtentNeighborExpand(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty) == nullptr);
    e->setIsHead(true);
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, false) == nullptr);
    e->setIsHead(false);
    CHECK_EQ(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, false), prev);
    extent_map.releaseExtent(nullptr, prev, extent_state_dirty);
    extent_map.deregisterBoundary(nullptr, next);
    next->setIsHead(false);
    registerExtent(extent_map, next, extent_state_dirty);

    /// Committed mismatch: rejected for coalescing, allowed for expanding.
    next->setCommitted(false);
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true) == nullptr);
    got = extent_map.tryAcquireExtentNeighborExpand(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty);
    CHECK_EQ(got, next);
    extent_map.releaseExtent(nullptr, next, extent_state_dirty);
    next->setCommitted(true);

    /// PAI mismatch.
    next->setAllocatorKind(EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR);
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, e, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true) == nullptr);
    next->setAllocatorKind(EXTENT_ALLOCATOR_PAGE_ALLOCATOR);

    /// No neighbor registered after `next`; and the page before `prev` is not mapped.
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, next, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true) == nullptr);
    CHECK(extent_map.tryAcquireExtentNeighbor(nullptr, prev, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, false) == nullptr);

    /// The rules directly.
    CHECK(extentNeighborHeadStateMergeable(true, false, true));
    CHECK(!extentNeighborHeadStateMergeable(false, true, true));
    CHECK(!extentNeighborHeadStateMergeable(true, false, false));
    CHECK(extentNeighborHeadStateMergeable(false, true, false));
    RadixTreeContents none = radix_tree_contents_cleared;
    CHECK(!extentCanAcquireNeighbor(e, none, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, extent_state_dirty, true, false));

    extent_map.deregisterBoundary(nullptr, prev);
    extent_map.deregisterBoundary(nullptr, e);
    extent_map.deregisterBoundary(nullptr, next);
    std::free(prev);
    std::free(e);
    std::free(next);
}

TEST(ExtentMap, FastAndBatchLookup)
{
    bootOnce();
    ExtentMap & extent_map = arena_extent_map_global;
    std::byte * base = region(4);
    SizeClassIdx bin_idx = 2;

    Extent * slab = makeExtent(base, 4 * PAGE, extent_state_active, true);
    slab->setSizeClassIdx(bin_idx);
    registerExtent(extent_map, slab, extent_state_active, bin_idx, true);
    extent_map.registerInterior(nullptr, slab, bin_idx);

    ThreadState * thread_state = new ThreadState;
    AllocContext alloc_context{};
    /// Cold cache: the fast path fails.
    CHECK(extent_map.allocContextTryLookupFast(*thread_state, base + 64, &alloc_context));
    /// A regular lookup through the thread's cache fills L1.
    CHECK_EQ(extent_map.extentLookup(thread_state, base + 64), slab);
    CHECK(!extent_map.allocContextTryLookupFast(*thread_state, base + PAGE + 64, &alloc_context));
    CHECK_EQ(alloc_context.size_class_idx, bin_idx);
    CHECK(alloc_context.slab);

    struct Ptrs
    {
        const void * ptrs[4];
    } ptrs = {{base, base + PAGE + 16, base + 2 * PAGE + 32, base + 3 * PAGE + 48}};
    struct Visited
    {
        int count = 0;
        bool all_ok = true;
        Extent * expected = nullptr;
        SizeClassIdx bin_idx = 0;
    } visited;
    visited.expected = slab;
    visited.bin_idx = bin_idx;
    ExtentMapBatchLookupResult result[4];
    extent_map.extentLookupBatch(
        *thread_state,
        4,
        [](void * context, size_t idx) -> const void * { return static_cast<Ptrs *>(context)->ptrs[idx]; },
        &ptrs,
        [](void * context, FullAllocContext * full)
        {
            auto * v = static_cast<Visited *>(context);
            ++v->count;
            v->all_ok = v->all_ok && full->extent == v->expected && full->slab && full->size_class_idx == v->bin_idx;
        },
        &visited,
        result);
    CHECK_EQ(visited.count, 4);
    CHECK(visited.all_ok);
    for (const auto & r : result)
        CHECK_EQ(r.extent, slab);

    extent_map.deregisterInterior(nullptr, slab);
    extent_map.deregisterBoundary(nullptr, slab);
    delete thread_state;
    std::free(slab);
}
