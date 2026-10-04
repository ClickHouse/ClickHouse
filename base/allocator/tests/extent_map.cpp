/// Tests of `ExtentMap` (`emap.c`): boundary/interior registration and lookups, remap, split and merge, neighbor
/// acquisition rules, the fast lookup through the thread's rtree cache, and the batch lookup. The map is the global
/// `arena_emap_global` on `b0`, initialized as at boot; extent addresses are fake (never dereferenced).

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

constinit ThreadState test_tsd;

void bootOnce()
{
    static bool booted = false;
    if (booted)
        return;
    REQUIRE(!pages::boot());
    REQUIRE(!baseBoot(nullptr));
    REQUIRE(!arena_emap_global.init(b0get(), true));
    booted = true;
}

Extent * newExtent()
{
    void * p = std::aligned_alloc(EDATA_ALIGNMENT, sizeof(Extent));
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
    e->init(0, addr, size, slab, SC_NSIZES, 1, state, false, committed, EXTENT_PAI_PAC, is_head ? EXTENT_IS_HEAD : EXTENT_NOT_HEAD);
    return e;
}

/// Registers with the state set to active (as `registerBoundary` requires), then sets the actual state.
void registerExtent(ExtentMap & emap, Extent * e, ExtentState state, szind_t szind = SC_NSIZES, bool slab = false)
{
    e->setState(extent_state_active);
    REQUIRE(!emap.registerBoundary(nullptr, e, szind, slab));
    if (state != extent_state_active)
        emap.updateEdataState(nullptr, e, state);
}

RadixTreeContents readContents(ExtentMap & emap, const void * ptr)
{
    RadixTreeContext fallback;
    RadixTreeContext * ctx = ThreadState::tsdnRtreeCtx(nullptr, &fallback);
    return emap.rtree.read(nullptr, ctx, reinterpret_cast<uintptr_t>(ptr));
}

}

TEST(ExtentMap, ThreadStateAccessors)
{
    static_assert(sizeof(RadixTreeContext) == 384);
    RadixTreeContext fallback;
    fallback.cache[0].leafkey = 77;
    CHECK(ThreadState::tsdnRtreeCtx(nullptr, &fallback) == &fallback);
    CHECK_EQ(fallback.cache[0].leafkey, RTREE_LEAFKEY_INVALID);
    CHECK(ThreadState::tsdnRtreeCtx(&test_tsd, &fallback) == &test_tsd.rtree_ctx);
    CHECK(test_tsd.rtreeCtx() == &test_tsd.rtree_ctx);
    CHECK_EQ(test_tsd.stateGet(), uint8_t(tsd_state_uninitialized));
    CHECK_EQ(test_tsd.arena_decay_ticker.read(), 1000);
    CHECK_EQ(test_tsd.binshards.binshard[0], uint8_t(UINT8_MAX));
    CHECK_EQ(test_tsd.binshards.binshard[1], 0u);
    CHECK_EQ(test_tsd.rtree_ctx.l2_cache[7].leafkey, RTREE_LEAFKEY_INVALID);
    test_tsd.prngState() = 5;
    CHECK_EQ(test_tsd.prng_state, 5u);
    test_tsd.reentrancyLevel() = 0;
    CHECK_EQ(test_tsd.reentrancy_level, 0);
}

TEST(ExtentMap, RegisterLookupDeregister)
{
    bootOnce();
    ExtentMap & emap = arena_emap_global;
    std::byte * base = region(0);

    /// One-page extent: both boundaries are the same element.
    Extent * one = makeExtent(base, PAGE, extent_state_active, false, true);
    registerExtent(emap, one, extent_state_active);
    CHECK_EQ(emap.edataLookup(nullptr, base), one);
    RadixTreeContents c = readContents(emap, base);
    CHECK_EQ(c.metadata.szind, SC_NSIZES);
    CHECK(!c.metadata.slab);
    CHECK(c.metadata.is_head);
    CHECK_EQ(c.metadata.state, extent_state_active);

    /// Large extent: first and last pages; interior pages are not registered.
    std::byte * large_addr = base + 4 * PAGE;
    size_t usize = SC_LARGE_MINCLASS + 2 * PAGE;
    Extent * large = makeExtent(large_addr, usize + sz_large_pad, extent_state_active);
    registerExtent(emap, large, extent_state_active);
    CHECK_EQ(emap.edataLookup(nullptr, large_addr), large);
    CHECK_EQ(emap.edataLookup(nullptr, large->last()), large);
    CHECK(emap.edataLookup(nullptr, large_addr + PAGE) == nullptr);

    /// Remap sets szind/slab at the head only (for non-slabs).
    szind_t szind = sz::sizeToIndex(usize);
    large->setSzind(szind);
    emap.remap(nullptr, large, szind, false);
    AllocContext alloc_ctx;
    emap.allocCtxLookup(nullptr, large_addr, &alloc_ctx);
    CHECK_EQ(alloc_ctx.szind, szind);
    CHECK(!alloc_ctx.slab);
    CHECK_EQ(alloc_ctx.usize, usize);
    CHECK_EQ(alloc_ctx.usizeGet(), usize);
    CHECK_EQ(readContents(emap, large->last()).metadata.szind, SC_NSIZES);
    /// Remap with SC_NSIZES is a no-op.
    emap.remap(nullptr, large, SC_NSIZES, false);
    CHECK_EQ(readContents(emap, large_addr).metadata.szind, szind);

    FullAllocContext full;
    emap.fullAllocCtxLookup(nullptr, large_addr, &full);
    CHECK_EQ(full.edata, large);
    CHECK_EQ(full.szind, szind);
    CHECK(!full.slab);

    /// Not mapped, but in an existing leaf: found with null edata. In a leaf that doesn't exist: not present.
    FullAllocContext missing{};
    CHECK(!emap.fullAllocCtxTryLookup(nullptr, base + 100 * PAGE, &missing));
    CHECK(missing.edata == nullptr);
    CHECK(emap.fullAllocCtxTryLookup(nullptr, region(1000), &missing));
    AllocContext missing_ctx;
    emap.allocCtxLookup(nullptr, base + 100 * PAGE, &missing_ctx);
    CHECK_EQ(missing_ctx.szind, 0u);
    CHECK_EQ(missing_ctx.usize, 0u);

    emap.deregisterBoundary(nullptr, large);
    emap.assertNotMapped(nullptr, large);
    c = readContents(emap, large_addr);
    CHECK(c.edata == nullptr);
    CHECK_EQ(c.metadata.szind, SC_NSIZES);
    emap.deregisterBoundary(nullptr, one);
    CHECK(emap.edataLookup(nullptr, base) == nullptr);
    std::free(one);
    std::free(large);
}

TEST(ExtentMap, Slab)
{
    bootOnce();
    ExtentMap & emap = arena_emap_global;
    std::byte * base = region(1);
    szind_t binind = 3;

    Extent * slab = makeExtent(base, 8 * PAGE, extent_state_active, true);
    slab->setSzind(binind);
    registerExtent(emap, slab, extent_state_active, binind, true);
    emap.registerInterior(nullptr, slab, binind);
    for (size_t i = 0; i < 8; ++i)
    {
        RadixTreeContents c = readContents(emap, base + i * PAGE + 16);
        CHECK_EQ(c.edata, slab);
        CHECK_EQ(c.metadata.szind, binind);
        CHECK(c.metadata.slab);
        CHECK_EQ(c.metadata.state, extent_state_active);
    }
    AllocContext alloc_ctx;
    emap.allocCtxLookup(nullptr, base + 5 * PAGE + 48, &alloc_ctx);
    CHECK(alloc_ctx.slab);
    CHECK_EQ(alloc_ctx.szind, binind);
    CHECK_EQ(alloc_ctx.usize, sz::indexToSize(binind));

    /// Remap of a slab also writes the last page.
    emap.remap(nullptr, slab, binind + 1, true);
    CHECK_EQ(readContents(emap, slab->last()).metadata.szind, binind + 1);
    CHECK_EQ(readContents(emap, base + PAGE).metadata.szind, binind);

    emap.deregisterInterior(nullptr, slab);
    for (size_t i = 1; i < 7; ++i)
    {
        RadixTreeContents c = readContents(emap, base + i * PAGE);
        CHECK(c.edata == nullptr);
        CHECK_EQ(c.metadata.szind, SC_NSIZES);
    }
    CHECK_EQ(emap.edataLookup(nullptr, base), slab);
    emap.deregisterBoundary(nullptr, slab);
    emap.assertNotMapped(nullptr, slab);

    /// Small slabs (<= 2 pages) have no interior.
    Extent * small = makeExtent(base + 16 * PAGE, 2 * PAGE, extent_state_active, true);
    registerExtent(emap, small, extent_state_active, binind, true);
    emap.deregisterInterior(nullptr, small);
    CHECK_EQ(emap.edataLookup(nullptr, small->last()), small);
    emap.deregisterBoundary(nullptr, small);
    std::free(slab);
    std::free(small);
}

TEST(ExtentMap, SplitMerge)
{
    bootOnce();
    ExtentMap & emap = arena_emap_global;
    std::byte * base = region(2);

    Extent * e = makeExtent(base, 8 * PAGE, extent_state_active);
    registerExtent(emap, e, extent_state_active);

    /// Split into 3 + 5 pages.
    Extent * trail = makeExtent(base + 3 * PAGE, 5 * PAGE, extent_state_active);
    ExtentMapPrepare prepare;
    CHECK(!emap.splitPrepare(nullptr, &prepare, e, 3 * PAGE, trail, 5 * PAGE));
    e->setSize(3 * PAGE);
    emap.splitCommit(nullptr, &prepare, e, 3 * PAGE, trail, 5 * PAGE);
    CHECK_EQ(emap.edataLookup(nullptr, base), e);
    CHECK_EQ(emap.edataLookup(nullptr, base + 2 * PAGE), e);
    CHECK_EQ(emap.edataLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(emap.edataLookup(nullptr, base + 7 * PAGE), trail);
    CHECK(emap.edataLookup(nullptr, base + 5 * PAGE) == nullptr);
    CHECK_EQ(readContents(emap, base).metadata.szind, SC_NSIZES);

    /// Split of a one-page lead: both lead elements are the same.
    Extent * trail2 = makeExtent(base + 4 * PAGE, 4 * PAGE, extent_state_active);
    CHECK(!emap.splitPrepare(nullptr, &prepare, trail, PAGE, trail2, 4 * PAGE));
    CHECK(prepare.lead_elm_a == prepare.lead_elm_b);
    trail->setSize(PAGE);
    emap.splitCommit(nullptr, &prepare, trail, PAGE, trail2, 4 * PAGE);
    CHECK_EQ(emap.edataLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(emap.edataLookup(nullptr, base + 4 * PAGE), trail2);
    CHECK_EQ(emap.edataLookup(nullptr, base + 7 * PAGE), trail2);

    /// Merge trail (1 page) + trail2 (4 pages): the inner boundaries are cleared.
    emap.mergePrepare(nullptr, &prepare, trail, trail2);
    CHECK(prepare.lead_elm_a == prepare.lead_elm_b);
    emap.mergeCommit(nullptr, &prepare, trail, trail2);
    trail->setSize(5 * PAGE);
    CHECK_EQ(emap.edataLookup(nullptr, base + 3 * PAGE), trail);
    CHECK_EQ(emap.edataLookup(nullptr, base + 7 * PAGE), trail);
    CHECK(emap.edataLookup(nullptr, base + 4 * PAGE) == nullptr);

    /// Merge e (3 pages) + trail (5 pages).
    emap.mergePrepare(nullptr, &prepare, e, trail);
    emap.mergeCommit(nullptr, &prepare, e, trail);
    e->setSize(8 * PAGE);
    CHECK_EQ(emap.edataLookup(nullptr, base), e);
    CHECK_EQ(emap.edataLookup(nullptr, base + 7 * PAGE), e);
    for (size_t i = 1; i < 7; ++i)
        CHECK(emap.edataLookup(nullptr, base + i * PAGE) == nullptr);
    RadixTreeContents c = readContents(emap, base + 2 * PAGE);
    CHECK_EQ(c.metadata.szind, SC_NSIZES);
    CHECK_EQ(c.metadata.state, extent_state_active);

    emap.deregisterBoundary(nullptr, e);
    std::free(e);
    std::free(trail);
    std::free(trail2);
}

TEST(ExtentMap, NeighborAcquisition)
{
    bootOnce();
    ExtentMap & emap = arena_emap_global;
    std::byte * base = region(3);

    /// [prev: dirty][e: active][next: dirty][far: retained, head]
    Extent * prev = makeExtent(base, 2 * PAGE, extent_state_dirty);
    Extent * e = makeExtent(base + 2 * PAGE, 2 * PAGE, extent_state_active);
    Extent * next = makeExtent(base + 4 * PAGE, 3 * PAGE, extent_state_dirty);
    registerExtent(emap, prev, extent_state_dirty);
    registerExtent(emap, e, extent_state_active);
    registerExtent(emap, next, extent_state_dirty);
    CHECK_EQ(readContents(emap, next->last()).metadata.state, extent_state_dirty);

    /// Wrong expected state.
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_muzzy, true) == nullptr);
    /// Forward.
    Extent * got = emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, true);
    CHECK_EQ(got, next);
    CHECK_EQ(next->state(), extent_state_merging);
    CHECK_EQ(readContents(emap, next->addr()).metadata.state, extent_state_merging);
    CHECK_EQ(readContents(emap, next->last()).metadata.state, extent_state_merging);
    /// Already acquired: the state no longer matches.
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, true) == nullptr);
    emap.releaseEdata(nullptr, next, extent_state_dirty);
    CHECK_EQ(next->state(), extent_state_dirty);
    CHECK_EQ(readContents(emap, next->last()).metadata.state, extent_state_dirty);

    /// Backward.
    got = emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, false);
    CHECK_EQ(got, prev);
    emap.releaseEdata(nullptr, prev, extent_state_dirty);

    /// Head states: no forward merge into a head neighbor; no backward merge when the extent itself is a head.
    emap.deregisterBoundary(nullptr, next);
    next->setIsHead(true);
    registerExtent(emap, next, extent_state_dirty);
    CHECK(readContents(emap, next->addr()).metadata.is_head);
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, true) == nullptr);
    CHECK(emap.tryAcquireEdataNeighborExpand(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty) == nullptr);
    e->setIsHead(true);
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, false) == nullptr);
    e->setIsHead(false);
    CHECK_EQ(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, false), prev);
    emap.releaseEdata(nullptr, prev, extent_state_dirty);
    emap.deregisterBoundary(nullptr, next);
    next->setIsHead(false);
    registerExtent(emap, next, extent_state_dirty);

    /// Committed mismatch: rejected for coalescing, allowed for expanding.
    next->setCommitted(false);
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, true) == nullptr);
    got = emap.tryAcquireEdataNeighborExpand(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty);
    CHECK_EQ(got, next);
    emap.releaseEdata(nullptr, next, extent_state_dirty);
    next->setCommitted(true);

    /// PAI mismatch.
    next->setPai(EXTENT_PAI_HPA);
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, e, EXTENT_PAI_PAC, extent_state_dirty, true) == nullptr);
    next->setPai(EXTENT_PAI_PAC);

    /// No neighbor registered after `next`; and the page before `prev` is not mapped.
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, next, EXTENT_PAI_PAC, extent_state_dirty, true) == nullptr);
    CHECK(emap.tryAcquireEdataNeighbor(nullptr, prev, EXTENT_PAI_PAC, extent_state_dirty, false) == nullptr);

    /// The rules directly.
    CHECK(extentNeighborHeadStateMergeable(true, false, true));
    CHECK(!extentNeighborHeadStateMergeable(false, true, true));
    CHECK(!extentNeighborHeadStateMergeable(true, false, false));
    CHECK(extentNeighborHeadStateMergeable(false, true, false));
    RadixTreeContents none = rtree_contents_cleared;
    CHECK(!extentCanAcquireNeighbor(e, none, EXTENT_PAI_PAC, extent_state_dirty, true, false));

    emap.deregisterBoundary(nullptr, prev);
    emap.deregisterBoundary(nullptr, e);
    emap.deregisterBoundary(nullptr, next);
    std::free(prev);
    std::free(e);
    std::free(next);
}

TEST(ExtentMap, FastAndBatchLookup)
{
    bootOnce();
    ExtentMap & emap = arena_emap_global;
    std::byte * base = region(4);
    szind_t binind = 2;

    Extent * slab = makeExtent(base, 4 * PAGE, extent_state_active, true);
    slab->setSzind(binind);
    registerExtent(emap, slab, extent_state_active, binind, true);
    emap.registerInterior(nullptr, slab, binind);

    ThreadState * tsd = new ThreadState;
    AllocContext alloc_ctx{};
    /// Cold cache: the fast path fails.
    CHECK(emap.allocCtxTryLookupFast(*tsd, base + 64, &alloc_ctx));
    /// A regular lookup through the thread's cache fills L1.
    CHECK_EQ(emap.edataLookup(tsd, base + 64), slab);
    CHECK(!emap.allocCtxTryLookupFast(*tsd, base + PAGE + 64, &alloc_ctx));
    CHECK_EQ(alloc_ctx.szind, binind);
    CHECK(alloc_ctx.slab);

    struct Ptrs
    {
        const void * ptrs[4];
    } ptrs = {{base, base + PAGE + 16, base + 2 * PAGE + 32, base + 3 * PAGE + 48}};
    struct Visited
    {
        int count = 0;
        bool all_ok = true;
        Extent * expected = nullptr;
        szind_t binind = 0;
    } visited;
    visited.expected = slab;
    visited.binind = binind;
    ExtentMapBatchLookupResult result[4];
    emap.edataLookupBatch(
        *tsd,
        4,
        [](void * ctx, size_t ind) -> const void * { return static_cast<Ptrs *>(ctx)->ptrs[ind]; },
        &ptrs,
        [](void * ctx, FullAllocContext * full)
        {
            auto * v = static_cast<Visited *>(ctx);
            ++v->count;
            v->all_ok = v->all_ok && full->edata == v->expected && full->slab && full->szind == v->binind;
        },
        &visited,
        result);
    CHECK_EQ(visited.count, 4);
    CHECK(visited.all_ok);
    for (const auto & r : result)
        CHECK_EQ(r.edata, slab);

    emap.deregisterInterior(nullptr, slab);
    emap.deregisterBoundary(nullptr, slab);
    delete tsd;
    std::free(slab);
}
