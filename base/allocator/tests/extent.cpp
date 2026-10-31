/// Tests of `Extent` (`edata_t`): the layout and the `packed_bits` positions for each page size (values measured from the C
/// build, `03-extents-extent_map-base.md` 0.1 and 1.3), the accessors, the initializers, the comparators, the heaps and the
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
    void * p = std::aligned_alloc(EXTENT_ALIGNMENT, n * sizeof(Extent));
    std::memset(p, 0, n * sizeof(Extent));
    return static_cast<Extent *>(p);
}

}

TEST(Extent, Layout)
{
    CHECK_EQ(offsetof(Extent, packed_bits), 0u);
    CHECK_EQ(offsetof(Extent, address), 8u);
    CHECK_EQ(offsetof(Extent, size_and_serial_number), 16u);
    CHECK_EQ(offsetof(Extent, base_size), 16u);
    CHECK_EQ(offsetof(Extent, unused_page_slab), 24u);
    CHECK_EQ(offsetof(Extent, serial_number), 32u);
    CHECK_EQ(offsetof(Extent, list_link_active), 40u);
    CHECK_EQ(offsetof(Extent, heap_link), 40u);
    CHECK_EQ(offsetof(Extent, available_link), 40u);
    CHECK_EQ(offsetof(Extent, list_link_inactive), 64u);
    CHECK_EQ(offsetof(Extent, slab_data), 64u);
    CHECK_EQ(offsetof(Extent, profiling_info), 64u);
    CHECK_EQ(sizeof(ExtentProfilingInfo), 56u);
    CHECK_EQ(offsetof(ExtentProfilingInfo, thread_context), 16u);
    CHECK_EQ(offsetof(ExtentProfilingInfo, fragmentation_link), 32u);
    CHECK_EQ(offsetof(ExtentProfilingInfo, fragmentation_tracked), 48u);

    if constexpr (LOG2_PAGE == 12)
    {
        CHECK_EQ(sizeof(SlabData), 64u);
        CHECK_EQ(sizeof(Extent), 128u);
    }
    else if constexpr (LOG2_PAGE == 14)
    {
        CHECK_EQ(sizeof(SlabData), 264u);
        CHECK_EQ(sizeof(Extent), 328u);
    }
    else
    {
        CHECK_EQ(sizeof(SlabData), 1048u);
        CHECK_EQ(sizeof(Extent), 1112u);
    }
    CHECK_EQ(EXTENT_ALIGNMENT, 128u);
    CHECK_EQ(EXTENT_SET_ENUMERATE_MAX_NUM, 32u);
}

TEST(Extent, BitPositions)
{
    CHECK_EQ(extent_bits::arena.shift, 0u);
    CHECK_EQ(extent_bits::arena.width, 12u);
    CHECK_EQ(extent_bits::slab.shift, 12u);
    CHECK_EQ(extent_bits::committed.shift, 13u);
    CHECK_EQ(extent_bits::allocator_kind.shift, 14u);
    CHECK_EQ(extent_bits::zeroed.shift, 15u);
    CHECK_EQ(extent_bits::guarded.shift, 16u);
    CHECK_EQ(extent_bits::state.shift, 17u);
    CHECK_EQ(extent_bits::state.width, 3u);
    CHECK_EQ(extent_bits::size_class_idx.shift, 20u);
    CHECK_EQ(extent_bits::size_class_idx.width, 8u);
    CHECK_EQ(extent_bits::num_free.shift, 28u);

    unsigned num_free_width = LOG2_PAGE == 12 ? 10 : (LOG2_PAGE == 14 ? 12 : 14);
    CHECK_EQ(extent_bits::num_free.width, num_free_width);
    CHECK_EQ(extent_bits::bin_shard.shift, 28 + num_free_width);
    CHECK_EQ(extent_bits::bin_shard.width, 6u);
    CHECK_EQ(extent_bits::is_head.shift, 34 + num_free_width);

    CHECK_EQ(extent_bits::arena.mask(), 0xfffu);
    CHECK_EQ(extent_bits::state.mask(), uint64_t(7) << 17);
    CHECK_EQ(EXTENT_SIZE_MASK, ~(PAGE - 1));
    CHECK_EQ(EXTENT_STRUCT_SERIAL_NUMBER_MASK, PAGE - 1);
}

TEST(Extent, Accessors)
{
    Extent * e = allocExtents(1);

    e->setArenaIdx(4094);
    CHECK_EQ(e->arenaIdx(), 4094u);
    CHECK_EQ(e->packed_bits, 4094u);

    e->setSlab(true);
    CHECK(e->slab());
    CHECK_EQ(e->packed_bits, 4094u | (1u << 12));
    e->setCommitted(true);
    e->setAllocatorKind(EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR);
    e->setZeroed(true);
    e->setGuarded(true);
    CHECK(e->committed());
    CHECK_EQ(e->allocatorKind(), EXTENT_ALLOCATOR_HUGE_PAGE_ALLOCATOR);
    CHECK(e->zeroed());
    CHECK(e->guarded());
    CHECK_EQ(e->packed_bits, uint64_t(0x1fffe));
    e->setAllocatorKind(EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    CHECK_EQ(e->allocatorKind(), EXTENT_ALLOCATOR_PAGE_ALLOCATOR);

    e->setState(extent_state_merging);
    CHECK_EQ(e->state(), extent_state_merging);
    CHECK_EQ((e->packed_bits >> 17) & 7, 5u);

    e->setSizeClassIdx(SIZE_CLASS_NUM_SIZES);
    CHECK_EQ(e->sizeClassIdxMaybeInvalid(), SIZE_CLASS_NUM_SIZES);
    e->setSizeClassIdx(3);
    CHECK_EQ(e->sizeClassIdx(), 3u);
    CHECK_EQ((e->packed_bits >> 20) & 0xff, 3u);

    e->setNumFreeBinShard(SIZE_CLASS_SLAB_MAX_REGIONS, 0);
    CHECK_EQ(e->numFree(), SIZE_CLASS_SLAB_MAX_REGIONS);
    CHECK_EQ(e->binShard(), 0u);
    e->numFreeDecrement();
    CHECK_EQ(e->numFree(), SIZE_CLASS_SLAB_MAX_REGIONS - 1);
    e->numFreeSub(10);
    CHECK_EQ(e->numFree(), SIZE_CLASS_SLAB_MAX_REGIONS - 11);
    e->numFreeIncrement();
    CHECK_EQ(e->numFree(), SIZE_CLASS_SLAB_MAX_REGIONS - 10);
    e->setNumFree(7);
    CHECK_EQ(e->numFree(), 7u);
    /// The other fields are intact.
    CHECK_EQ(e->arenaIdx(), 4094u);
    CHECK_EQ(e->sizeClassIdx(), 3u);
    CHECK_EQ(e->state(), extent_state_merging);

    e->setIsHead(true);
    CHECK(e->isHead());
    CHECK_EQ(e->packed_bits >> extent_bits::is_head.shift, 1u);
    e->setIsHead(false);
    CHECK(!e->isHead());

    /// Size and esn share a word.
    e->setStructSerialNumber(PAGE + 5);
    CHECK_EQ(e->structSerialNumber(), 5u);
    e->setSize(10 * PAGE);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->structSerialNumber(), 5u);
    e->setStructSerialNumber(PAGE - 1);
    CHECK_EQ(e->size(), 10 * PAGE);
    CHECK_EQ(e->structSerialNumber(), PAGE - 1);
    CHECK_EQ(e->size_and_serial_number, 11 * PAGE - 1);
    e->setBaseSize(12345);
    CHECK_EQ(e->baseSize(), 12345u);

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

    e->setSerialNumber(42);
    CHECK_EQ(e->serialNumber(), 42u);

    std::free(e);
}

TEST(Extent, UsableSize)
{
    Extent * e = allocExtents(1);
    e->init(
        0,
        reinterpret_cast<void *>(uintptr_t(1) << 40),
        8 * PAGE,
        false,
        0,
        0,
        extent_state_active,
        false,
        true,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        EXTENT_IS_HEAD);
    /// Small: from the index.
    e->setSizeClassIdx(5);
    CHECK_EQ(e->usableSize(), size_classes::indexToSize(5));
    /// Large with disabled large size classes (the default): from the size.
    CHECK(size_classes::largeSizeClassesDisabled());
    e->setSize(SIZE_CLASS_LARGE_MIN_CLASS + 3 * PAGE + large_pad);
    e->setSizeClassIdx(size_classes::sizeToIndex(SIZE_CLASS_LARGE_MIN_CLASS + 3 * PAGE));
    CHECK_EQ(e->usableSize(), SIZE_CLASS_LARGE_MIN_CLASS + 3 * PAGE);
    std::free(e);
}

TEST(Extent, Profiling)
{
    Extent * e = allocExtents(1);
    auto * thread_context = reinterpret_cast<ProfilingThreadContext *>(uintptr_t(0x1000));
    e->setProfilingThreadContext(thread_context);
    CHECK_EQ(e->profilingThreadContext(), thread_context);
    auto * recent = reinterpret_cast<ProfilingRecent *>(uintptr_t(0x2000));
    e->setProfilingRecentAllocDontCallDirectly(recent);
    CHECK_EQ(e->profilingRecentAllocGetDontCallDirectly(), recent);
    Nanoseconds t = Nanoseconds::fromNanoseconds(123456789);
    e->setProfilingAllocTime(&t);
    CHECK_EQ(e->profilingAllocTime()->ns(), 123456789u);
    e->setProfilingAllocSize(777);
    CHECK_EQ(e->profilingAllocSize(), 777u);
    e->setProfilingFragmentationTracked(true);
    CHECK(e->profilingFragmentationTracked());
    std::free(e);
}

TEST(Extent, Init)
{
    Extent * e = allocExtents(1);
    e->setStructSerialNumber(17);
    e->setProfilingThreadContext(reinterpret_cast<ProfilingThreadContext *>(uintptr_t(0x1000)));
    e->setGuarded(true);
    auto * addr = reinterpret_cast<void *>(uintptr_t(1) << 40);
    e->init(7, addr, 3 * PAGE, true, 2, 99, extent_state_dirty, true, true, EXTENT_ALLOCATOR_PAGE_ALLOCATOR, EXTENT_IS_HEAD);
    CHECK_EQ(e->arenaIdx(), 7u);
    CHECK_EQ(e->addr(), addr);
    CHECK_EQ(e->size(), 3 * PAGE);
    CHECK_EQ(e->structSerialNumber(), 17u); /// Preserved.
    CHECK(e->slab());
    CHECK_EQ(e->sizeClassIdx(), 2u);
    CHECK_EQ(e->serialNumber(), 99u);
    CHECK_EQ(e->state(), extent_state_dirty);
    CHECK(!e->guarded());
    CHECK(e->zeroed());
    CHECK(e->committed());
    CHECK_EQ(e->allocatorKind(), EXTENT_ALLOCATOR_PAGE_ALLOCATOR);
    CHECK(e->isHead());
    CHECK(e->profilingThreadContext() == nullptr);

    /// `edata_binit`: arena 4095, szind SIZE_CLASS_NUM_SIZES, guarded = reused, does not touch is_head.
    e->initBase(addr, 1000, 5, true);
    CHECK_EQ(unsigned(e->packed_bits & 0xfff), 4095u);
    CHECK_EQ(e->baseSize(), 1000u);
    CHECK(!e->slab());
    CHECK_EQ(e->sizeClassIdxMaybeInvalid(), SIZE_CLASS_NUM_SIZES);
    CHECK_EQ(e->serialNumber(), 5u);
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
    e[0].setSerialNumber(1);
    e[0].setAddr(base + PAGE);
    e[1].setSerialNumber(1);
    e[1].setAddr(base);
    e[2].setSerialNumber(0);
    e[2].setAddr(base + 10 * PAGE);
    e[3].setSerialNumber(1);
    e[3].setAddr(base + PAGE);

    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[0], &e[1]), 1);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[1], &e[0]), -1);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[0], &e[2]), 1);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[2], &e[1]), -1);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[0], &e[3]), 0);
    /// The branchless form: 2 * sign(sn) + sign(addr).
    e[2].setAddr(base);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[2], &e[0]), -3);
    CHECK_EQ(Extent::compareSerialNumberAndAddress(&e[0], &e[2]), 3);

    e[0].setStructSerialNumber(3);
    e[1].setStructSerialNumber(3);
    e[2].setStructSerialNumber(1);
    CHECK_EQ(Extent::compareStructSerialNumber(&e[0], &e[1]), 0);
    CHECK_EQ(Extent::compareExtentAddress(&e[0], &e[1]), -1);
    CHECK_EQ(Extent::compareStructSerialNumberAndAddress(&e[0], &e[1]), -1);
    CHECK_EQ(Extent::compareStructSerialNumberAndAddress(&e[1], &e[0]), 1);
    CHECK_EQ(Extent::compareStructSerialNumberAndAddress(&e[0], &e[2]), 1);
    CHECK_EQ(Extent::compareStructSerialNumberAndAddress(&e[2], &e[0]), -1);
    CHECK_EQ(Extent::compareStructSerialNumberAndAddress(&e[0], &e[0]), 0);

    ExtentComparisonSummary s = e[3].comparisonSummary();
    CHECK_EQ(s.serial_number, 1u);
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
        e[j].setSerialNumber(j % 4);
        e[j].setAddr(base + j * PAGE);
        heap.insert(&e[j]);
    }
    uint64_t prev_serial_number = 0;
    uintptr_t prev_addr = 0;
    for (size_t i = 0; i < n; ++i)
    {
        Extent * first = heap.removeFirst();
        REQUIRE(first != nullptr);
        uintptr_t a = reinterpret_cast<uintptr_t>(first->addr());
        CHECK(first->serialNumber() > prev_serial_number || (first->serialNumber() == prev_serial_number && a > prev_addr) || i == 0);
        prev_serial_number = first->serialNumber();
        prev_addr = a;
    }
    CHECK(heap.empty());

    /// The avail heap orders by (esn, structure address).
    ExtentAvailableHeap available;
    available.init();
    for (size_t i = n; i-- > 0;)
    {
        e[i].setStructSerialNumber(i % 3);
        available.insert(&e[i]);
    }
    for (size_t i = 0; i < n; ++i)
    {
        Extent * first = available.removeFirst();
        size_t expected = (i < 22) ? 3 * i : (i < 43 ? 3 * (i - 22) + 1 : 3 * (i - 43) + 2);
        CHECK_EQ(first, &e[expected]);
    }

    /// Enumeration of up to EXTENT_SET_ENUMERATE_MAX_NUM nodes.
    heap.init();
    for (size_t i = 0; i < n; ++i)
        heap.insert(&e[i]);
    ExtentHeapEnumerateHelper helper;
    heap.enumeratePrepare(helper, EXTENT_SET_ENUMERATE_MAX_NUM, EXTENT_SET_ENUMERATE_MAX_NUM);
    size_t visited = 0;
    while (heap.enumerateNext(helper) != nullptr)
        ++visited;
    CHECK_EQ(visited, size_t(EXTENT_SET_ENUMERATE_MAX_NUM));
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
    ExtentListFragmentation fragmentation;
    fragmentation.init();
    CHECK(fragmentation.empty());
    for (i = 0; i < n; ++i)
        fragmentation.append(&e[i]);
    fragmentation.remove(&e[0]);
    fragmentation.prepend(&e[0]);
    CHECK_EQ(fragmentation.first(), &e[0]);
    fragmentation.remove(&e[0]);
    Extent * x = fragmentation.first();
    for (i = 1; i < n; ++i)
    {
        CHECK_EQ(x, &e[i]);
        x = fragmentation.next(x);
    }
    CHECK(x == nullptr);
    CHECK_EQ(fragmentation.last(), &e[n - 1]);
    fragmentation.remove(&e[n - 1]);
    CHECK_EQ(fragmentation.last(), &e[n - 2]);
    fragmentation.remove(&e[3]);
    size_t expected[] = {1, 2, 4, 5, 6};
    i = 0;
    fragmentation.forEach([&](Extent * y) { CHECK_EQ(y, &e[expected[i++]]); });
    CHECK_EQ(i, 5u);
    for (size_t k : expected)
        fragmentation.remove(&e[k]);
    CHECK(fragmentation.empty());
    std::free(e);
}
