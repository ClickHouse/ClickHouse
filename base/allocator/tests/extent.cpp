/// Tests of `Extent` (`edata_t`): the layout and the `e_bits` positions for each page size (values measured from the C
/// build, `03-extents-emap-base.md` 0.1 and 1.3), the accessors, the initializers, the comparators, the heaps and the
/// lists.

#include <allocator/Extent.h>

#include "Test.h"

#include <cstddef>
#include <cstdlib>
#include <cstring>

using namespace jemalloc;

namespace
{

Extent * allocExtents(size_t n)
{
    void * p = std::aligned_alloc(EDATA_ALIGNMENT, n * sizeof(Extent));
    std::memset(p, 0, n * sizeof(Extent));
    return static_cast<Extent *>(p);
}

}

TEST(Extent, Layout)
{
    CHECK_EQ(offsetof(Extent, e_bits), 0u);
    CHECK_EQ(offsetof(Extent, e_addr), 8u);
    CHECK_EQ(offsetof(Extent, e_size_esn), 16u);
    CHECK_EQ(offsetof(Extent, e_bsize), 16u);
    CHECK_EQ(offsetof(Extent, e_ps), 24u);
    CHECK_EQ(offsetof(Extent, e_sn), 32u);
    CHECK_EQ(offsetof(Extent, ql_link_active), 40u);
    CHECK_EQ(offsetof(Extent, heap_link), 40u);
    CHECK_EQ(offsetof(Extent, avail_link), 40u);
    CHECK_EQ(offsetof(Extent, ql_link_inactive), 64u);
    CHECK_EQ(offsetof(Extent, e_slab_data), 64u);
    CHECK_EQ(offsetof(Extent, e_prof_info), 64u);
    CHECK_EQ(sizeof(ExtentProfInfo), 56u);
    CHECK_EQ(offsetof(ExtentProfInfo, e_prof_tctx), 16u);
    CHECK_EQ(offsetof(ExtentProfInfo, e_prof_frag_link), 32u);
    CHECK_EQ(offsetof(ExtentProfInfo, e_prof_frag_tracked), 48u);

    if constexpr (LG_PAGE == 12)
    {
        CHECK_EQ(sizeof(SlabData), 64u);
        CHECK_EQ(sizeof(Extent), 128u);
    }
    else if constexpr (LG_PAGE == 14)
    {
        CHECK_EQ(sizeof(SlabData), 264u);
        CHECK_EQ(sizeof(Extent), 328u);
    }
    else
    {
        CHECK_EQ(sizeof(SlabData), 1048u);
        CHECK_EQ(sizeof(Extent), 1112u);
    }
    CHECK_EQ(EDATA_ALIGNMENT, 128u);
    CHECK_EQ(ESET_ENUMERATE_MAX_NUM, 32u);
}

TEST(Extent, BitPositions)
{
    CHECK_EQ(extent_bits::arena.shift, 0u);
    CHECK_EQ(extent_bits::arena.width, 12u);
    CHECK_EQ(extent_bits::slab.shift, 12u);
    CHECK_EQ(extent_bits::committed.shift, 13u);
    CHECK_EQ(extent_bits::pai.shift, 14u);
    CHECK_EQ(extent_bits::zeroed.shift, 15u);
    CHECK_EQ(extent_bits::guarded.shift, 16u);
    CHECK_EQ(extent_bits::state.shift, 17u);
    CHECK_EQ(extent_bits::state.width, 3u);
    CHECK_EQ(extent_bits::szind.shift, 20u);
    CHECK_EQ(extent_bits::szind.width, 8u);
    CHECK_EQ(extent_bits::nfree.shift, 28u);

    unsigned nfree_width = LG_PAGE == 12 ? 10 : (LG_PAGE == 14 ? 12 : 14);
    CHECK_EQ(extent_bits::nfree.width, nfree_width);
    CHECK_EQ(extent_bits::binshard.shift, 28 + nfree_width);
    CHECK_EQ(extent_bits::binshard.width, 6u);
    CHECK_EQ(extent_bits::is_head.shift, 34 + nfree_width);

    CHECK_EQ(extent_bits::arena.mask(), 0xfffu);
    CHECK_EQ(extent_bits::state.mask(), uint64_t(7) << 17);
    CHECK_EQ(EDATA_SIZE_MASK, ~(PAGE - 1));
    CHECK_EQ(EDATA_ESN_MASK, PAGE - 1);
}

TEST(Extent, Accessors)
{
    Extent * e = allocExtents(1);

    e->setArenaInd(4094);
    CHECK_EQ(e->arenaInd(), 4094u);
    CHECK_EQ(e->e_bits, 4094u);

    e->setSlab(true);
    CHECK(e->slab());
    CHECK_EQ(e->e_bits, 4094u | (1u << 12));
    e->setCommitted(true);
    e->setPai(EXTENT_PAI_HPA);
    e->setZeroed(true);
    e->setGuarded(true);
    CHECK(e->committed());
    CHECK_EQ(e->pai(), EXTENT_PAI_HPA);
    CHECK(e->zeroed());
    CHECK(e->guarded());
    CHECK_EQ(e->e_bits, uint64_t(0x1fffe));
    e->setPai(EXTENT_PAI_PAC);
    CHECK_EQ(e->pai(), EXTENT_PAI_PAC);

    e->setState(extent_state_merging);
    CHECK_EQ(e->state(), extent_state_merging);
    CHECK_EQ((e->e_bits >> 17) & 7, 5u);

    e->setSzind(SC_NSIZES);
    CHECK_EQ(e->szindMaybeInvalid(), SC_NSIZES);
    e->setSzind(3);
    CHECK_EQ(e->szind(), 3u);
    CHECK_EQ((e->e_bits >> 20) & 0xff, 3u);

    e->setNfreeBinshard(SC_SLAB_MAXREGS, 0);
    CHECK_EQ(e->nfree(), SC_SLAB_MAXREGS);
    CHECK_EQ(e->binshard(), 0u);
    e->nfreeDec();
    CHECK_EQ(e->nfree(), SC_SLAB_MAXREGS - 1);
    e->nfreeSub(10);
    CHECK_EQ(e->nfree(), SC_SLAB_MAXREGS - 11);
    e->nfreeInc();
    CHECK_EQ(e->nfree(), SC_SLAB_MAXREGS - 10);
    e->setNfree(7);
    CHECK_EQ(e->nfree(), 7u);
    /// The other fields are intact.
    CHECK_EQ(e->arenaInd(), 4094u);
    CHECK_EQ(e->szind(), 3u);
    CHECK_EQ(e->state(), extent_state_merging);

    e->setIsHead(true);
    CHECK(e->isHead());
    CHECK_EQ(e->e_bits >> extent_bits::is_head.shift, 1u);
    e->setIsHead(false);
    CHECK(!e->isHead());

    /// Size and esn share a word.
    e->setEsn(PAGE + 5);
    CHECK_EQ(e->esn(), 5u);
    e->setSize(10 * PAGE);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->esn(), 5u);
    e->setEsn(PAGE - 1);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->esn(), PAGE - 1);
    CHECK_EQ(e->e_size_esn, 11 * PAGE - 1);
    e->setBsize(12345);
    CHECK_EQ(e->bsize(), 12345u);

    /// Addresses.
    e->setSlab(false);
    auto * addr = reinterpret_cast<std::byte *>(uintptr_t(1) << 40);
    e->setAddr(addr + 64);
    e->setSize(4 * PAGE);
    CHECK_EQ(e->addr(), static_cast<void *>(addr + 64));
    CHECK_EQ(e->base(), static_cast<void *>(addr));
    CHECK_EQ(e->before(), static_cast<void *>(addr - PAGE));
    CHECK_EQ(e->last(), static_cast<void *>(addr + 3 * PAGE));
    CHECK_EQ(e->past(), static_cast<void *>(addr + 4 * PAGE));

    e->setSn(42);
    CHECK_EQ(e->sn(), 42u);

    std::free(e);
}

TEST(Extent, Usize)
{
    Extent * e = allocExtents(1);
    e->init(0, reinterpret_cast<void *>(uintptr_t(1) << 40), 8 * PAGE, false, 0, 0, extent_state_active, false, true,
        EXTENT_PAI_PAC, EXTENT_IS_HEAD);
    /// Small: from the index.
    e->setSzind(5);
    CHECK_EQ(e->usize(), sz::indexToSize(5));
    /// Large with disabled large size classes (the default): from the size.
    CHECK(sz::largeSizeClassesDisabled());
    e->setSize(SC_LARGE_MINCLASS + 3 * PAGE + sz_large_pad);
    e->setSzind(sz::sizeToIndex(SC_LARGE_MINCLASS + 3 * PAGE));
    CHECK_EQ(e->usize(), SC_LARGE_MINCLASS + 3 * PAGE);
    std::free(e);
}

TEST(Extent, Prof)
{
    Extent * e = allocExtents(1);
    auto * tctx = reinterpret_cast<ProfThreadContext *>(uintptr_t(0x1000));
    e->setProfTctx(tctx);
    CHECK_EQ(e->profTctx(), tctx);
    auto * recent = reinterpret_cast<ProfRecent *>(uintptr_t(0x2000));
    e->setProfRecentAllocDontCallDirectly(recent);
    CHECK_EQ(e->profRecentAllocGetDontCallDirectly(), recent);
    NsTime t = NsTime::fromNs(123456789);
    e->setProfAllocTime(&t);
    CHECK_EQ(e->profAllocTime()->ns(), 123456789u);
    e->setProfAllocSize(777);
    CHECK_EQ(e->profAllocSize(), 777u);
    e->setProfFragTracked(true);
    CHECK(e->profFragTracked());
    std::free(e);
}

TEST(Extent, Init)
{
    Extent * e = allocExtents(1);
    e->setEsn(17);
    e->setProfTctx(reinterpret_cast<ProfThreadContext *>(uintptr_t(0x1000)));
    e->setGuarded(true);
    auto * addr = reinterpret_cast<void *>(uintptr_t(1) << 40);
    e->init(7, addr, 3 * PAGE, true, 2, 99, extent_state_dirty, true, true, EXTENT_PAI_PAC, EXTENT_IS_HEAD);
    CHECK_EQ(e->arenaInd(), 7u);
    CHECK_EQ(e->addr(), addr);
    CHECK_EQ(e->size(), 3 * PAGE);
    CHECK_EQ(e->esn(), 17u); /// Preserved.
    CHECK(e->slab());
    CHECK_EQ(e->szind(), 2u);
    CHECK_EQ(e->sn(), 99u);
    CHECK_EQ(e->state(), extent_state_dirty);
    CHECK(!e->guarded());
    CHECK(e->zeroed());
    CHECK(e->committed());
    CHECK_EQ(e->pai(), EXTENT_PAI_PAC);
    CHECK(e->isHead());
    CHECK(e->profTctx() == nullptr);

    /// `edata_binit`: arena 4095, szind SC_NSIZES, guarded = reused, does not touch is_head.
    e->initBase(addr, 1000, 5, true);
    CHECK_EQ(unsigned(e->e_bits & 0xfff), 4095u);
    CHECK_EQ(e->bsize(), 1000u);
    CHECK(!e->slab());
    CHECK_EQ(e->szindMaybeInvalid(), SC_NSIZES);
    CHECK_EQ(e->sn(), 5u);
    CHECK_EQ(e->state(), extent_state_active);
    CHECK(e->guarded());
    CHECK(e->zeroed());
    CHECK(e->committed());
    CHECK(e->isHead());
    e->initBase(addr, 1000, 5, false);
    CHECK(!e->guarded());

    CHECK(!extentStateInTransition(extent_state_retained));
    CHECK(extentStateInTransition(extent_state_transition));
    CHECK(extentStateInTransition(extent_state_merging));
    std::free(e);
}

TEST(Extent, Comparators)
{
    Extent * e = allocExtents(4);
    auto * base = reinterpret_cast<std::byte *>(uintptr_t(1) << 40);
    e[0].setSn(1);
    e[0].setAddr(base + PAGE);
    e[1].setSn(1);
    e[1].setAddr(base);
    e[2].setSn(0);
    e[2].setAddr(base + 10 * PAGE);
    e[3].setSn(1);
    e[3].setAddr(base + PAGE);

    CHECK_EQ(Extent::compareSnad(&e[0], &e[1]), 1);
    CHECK_EQ(Extent::compareSnad(&e[1], &e[0]), -1);
    CHECK_EQ(Extent::compareSnad(&e[0], &e[2]), 1);
    CHECK_EQ(Extent::compareSnad(&e[2], &e[1]), -1);
    CHECK_EQ(Extent::compareSnad(&e[0], &e[3]), 0);
    /// The branchless form: 2 * sign(sn) + sign(addr).
    e[2].setAddr(base);
    CHECK_EQ(Extent::compareSnad(&e[2], &e[0]), -3);
    CHECK_EQ(Extent::compareSnad(&e[0], &e[2]), 3);

    e[0].setEsn(3);
    e[1].setEsn(3);
    e[2].setEsn(1);
    CHECK_EQ(Extent::compareEsn(&e[0], &e[1]), 0);
    CHECK_EQ(Extent::compareEad(&e[0], &e[1]), -1);
    CHECK_EQ(Extent::compareEsnead(&e[0], &e[1]), -1);
    CHECK_EQ(Extent::compareEsnead(&e[1], &e[0]), 1);
    CHECK_EQ(Extent::compareEsnead(&e[0], &e[2]), 1);
    CHECK_EQ(Extent::compareEsnead(&e[2], &e[0]), -1);
    CHECK_EQ(Extent::compareEsnead(&e[0], &e[0]), 0);

    ExtentCmpSummary s = e[3].cmpSummary();
    CHECK_EQ(s.sn, 1u);
    CHECK_EQ(s.addr, reinterpret_cast<uintptr_t>(base + PAGE));
    std::free(e);
}

TEST(Extent, Heaps)
{
    constexpr size_t n = 64;
    Extent * e = allocExtents(n);
    auto * base = reinterpret_cast<std::byte *>(uintptr_t(1) << 40);

    ExtentHeap heap;
    heap.init();
    for (size_t i = 0; i < n; ++i)
    {
        size_t j = (i * 37) % n;
        e[j].setSn(j % 4);
        e[j].setAddr(base + j * PAGE);
        heap.insert(&e[j]);
    }
    uint64_t prev_sn = 0;
    uintptr_t prev_addr = 0;
    for (size_t i = 0; i < n; ++i)
    {
        Extent * first = heap.removeFirst();
        REQUIRE(first != nullptr);
        uintptr_t a = reinterpret_cast<uintptr_t>(first->addr());
        CHECK(first->sn() > prev_sn || (first->sn() == prev_sn && a > prev_addr) || i == 0);
        prev_sn = first->sn();
        prev_addr = a;
    }
    CHECK(heap.empty());

    /// The avail heap orders by (esn, structure address).
    ExtentAvailHeap avail;
    avail.init();
    for (size_t i = n; i-- > 0;)
    {
        e[i].setEsn(i % 3);
        avail.insert(&e[i]);
    }
    for (size_t i = 0; i < n; ++i)
    {
        Extent * first = avail.removeFirst();
        size_t expected = (i < 22) ? 3 * i : (i < 43 ? 3 * (i - 22) + 1 : 3 * (i - 43) + 2);
        CHECK_EQ(first, &e[expected]);
    }

    /// Enumeration of up to ESET_ENUMERATE_MAX_NUM nodes.
    heap.init();
    for (size_t i = 0; i < n; ++i)
        heap.insert(&e[i]);
    ExtentHeapEnumerateHelper helper;
    heap.enumeratePrepare(helper, ESET_ENUMERATE_MAX_NUM, ESET_ENUMERATE_MAX_NUM);
    size_t visited = 0;
    while (heap.enumerateNext(helper) != nullptr)
        ++visited;
    CHECK_EQ(visited, size_t(ESET_ENUMERATE_MAX_NUM));
    std::free(e);
}

TEST(Extent, Lists)
{
    constexpr size_t n = 8;
    Extent * e = allocExtents(n);

    ExtentListActive active;
    active.init();
    ExtentListInactive inactive;
    inactive.init();
    for (size_t i = 0; i < n; ++i)
    {
        active.append(&e[i]);
        inactive.prepend(&e[i]);
    }
    size_t i = 0;
    for (Extent * x : active)
        CHECK_EQ(x, &e[i++]);
    i = n;
    for (Extent * x : inactive)
        CHECK_EQ(x, &e[--i]);

    /// The frag list shares the storage with the inactive link (a union), so use it alone.
    ExtentListFrag frag;
    frag.init();
    CHECK(frag.empty());
    for (i = 0; i < n; ++i)
        frag.append(&e[i]);
    frag.remove(&e[0]);
    frag.prepend(&e[0]);
    CHECK_EQ(frag.first(), &e[0]);
    frag.remove(&e[0]);
    Extent * x = frag.first();
    for (i = 1; i < n; ++i)
    {
        CHECK_EQ(x, &e[i]);
        x = frag.next(x);
    }
    CHECK(x == nullptr);
    CHECK_EQ(frag.last(), &e[n - 1]);
    frag.remove(&e[n - 1]);
    CHECK_EQ(frag.last(), &e[n - 2]);
    frag.remove(&e[3]);
    size_t expected[] = {1, 2, 4, 5, 6};
    i = 0;
    frag.forEach([&](Extent * y) { CHECK_EQ(y, &e[expected[i++]]); });
    CHECK_EQ(i, 5u);
    for (size_t k : expected)
        frag.remove(&e[k]);
    CHECK(frag.empty());
    std::free(e);
}
