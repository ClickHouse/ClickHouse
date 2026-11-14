/// Unit tests of `Base` (base.c) and `ExtentPool` (edata_cache.c). The exact sequences are compared with the C
/// implementation in base_oracle.cpp; here the values that follow directly from jemalloc's formulas are pinned.

#include <allocator/Base.h>
#include <allocator/ExtentPool.h>
#include <allocator/Pages.h>

#include "Test.h"

using namespace jemalloc;

namespace
{

void bootOnce()
{
    static bool booted = false;
    if (!booted)
    {
        REQUIRE(!pages::boot());
        booted = true;
    }
}

struct Stats
{
    size_t allocated;
    size_t extent_allocated;
    size_t radix_tree_allocated;
    size_t resident;
    size_t mapped;
    size_t num_transparent_huge_pages;
};

Stats getStats(Base * base)
{
    Stats s;
    base->statsGet(
        nullptr, &s.allocated, &s.extent_allocated, &s.radix_tree_allocated, &s.resident, &s.mapped, &s.num_transparent_huge_pages);
    return s;
}

int countBlocks(const Base * base)
{
    int n = 0;
    for (const BaseBlock * b = base->blocksList(); b; b = b->next)
        ++n;
    return n;
}

bool allZero(const void * p, size_t size)
{
    const unsigned char * c = static_cast<const unsigned char *>(p);
    for (size_t i = 0; i < size; ++i)
        if (c[i])
            return false;
    return true;
}

}

TEST(Base, Create)
{
    bootOnce();
    Base * base = Base::create(nullptr, 3, &extent_hooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    CHECK_EQ(base->idxGet(), 3u);
    CHECK(base->extentHooksGet()->areDefault());
    CHECK(base->extentHooksGetForMetadata()->areDefault());
    CHECK_EQ(base->extentHooksGet()->idxGet(), 3u);

    const BaseBlock * block = base->blocksList();
    REQUIRE(block != nullptr);
    CHECK(block->next == nullptr);
    CHECK_EQ(block->size, BASE_BLOCK_MIN_ALIGN);
    CHECK_EQ(reinterpret_cast<uintptr_t>(block) % BASE_BLOCK_MIN_ALIGN, 0u);
    /// The `Base` follows the block header, CACHELINE-aligned.
    CHECK_EQ(reinterpret_cast<uintptr_t>(base), alignmentCeiling(reinterpret_cast<uintptr_t>(block) + sizeof(BaseBlock), CACHE_LINE));

    Stats s = getStats(base);
    size_t base_size = alignmentCeiling(sizeof(Base), CACHE_LINE);
    CHECK_EQ(s.allocated, sizeof(BaseBlock) + base_size);
    CHECK_EQ(s.extent_allocated, 0u);
    CHECK_EQ(s.radix_tree_allocated, 0u);
    CHECK_EQ(
        s.resident, pageCeiling(sizeof(BaseBlock) + (alignmentCeiling(sizeof(BaseBlock), CACHE_LINE) - sizeof(BaseBlock)) + base_size));
    CHECK_EQ(s.mapped, BASE_BLOCK_MIN_ALIGN);
    CHECK_EQ(s.num_transparent_huge_pages, 0u);

    base->destroy(nullptr);
}

TEST(Base, BumpAllocation)
{
    bootOnce();
    Base * base = Base::create(nullptr, 1, &extent_hooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    Stats s0 = getStats(base);

    /// Sizes are rounded up to the alignment (at least QUANTUM); consecutive allocations are contiguous.
    char * a = static_cast<char *>(base->alloc(nullptr, 1, 1));
    char * b = static_cast<char *>(base->alloc(nullptr, 17, 16));
    char * c = static_cast<char *>(base->alloc(nullptr, 64, 64));
    REQUIRE(a && b && c);
    CHECK_EQ(reinterpret_cast<uintptr_t>(a) % QUANTUM, 0u);
    CHECK(b == a + 16);
    CHECK_EQ(reinterpret_cast<uintptr_t>(c) % 64, 0u);
    CHECK(c == reinterpret_cast<char *>(alignmentCeiling(reinterpret_cast<uintptr_t>(b + 32), 64)));
    CHECK(allZero(a, 16));
    Stats s1 = getStats(base);
    CHECK_EQ(s1.allocated, s0.allocated + 16 + 32 + 64);

    /// Page-aligned allocation: resident grows by one page per crossed page boundary.
    void * p = base->alloc(nullptr, PAGE, PAGE);
    REQUIRE(p);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p) % PAGE, 0u);
    Stats s2 = getStats(base);
    CHECK_EQ(s2.allocated, s1.allocated + PAGE);

    /// Extents: EXTENT_ALIGNMENT-aligned, `struct_serial_number` = the serial number of the block (0 for the first one).
    Extent * e = base->allocExtent(nullptr);
    REQUIRE(e);
    CHECK_EQ(reinterpret_cast<uintptr_t>(e) % EXTENT_ALIGNMENT, 0u);
    CHECK_EQ(e->structSerialNumber(), 0u);
    Stats s3 = getStats(base);
    CHECK_EQ(s3.extent_allocated, alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT));
    CHECK_EQ(s3.allocated, s2.allocated + alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT));

    void * r = base->allocRadixTree(nullptr, 100);
    REQUIRE(r);
    CHECK_EQ(reinterpret_cast<uintptr_t>(r) % CACHE_LINE, 0u);
    CHECK_EQ(getStats(base).radix_tree_allocated, 128u);

    /// A request that does not fit into the first block maps a new one; extents from it have esn 1.
    CHECK_EQ(countBlocks(base), 1);
    void * big = base->alloc(nullptr, BASE_BLOCK_MIN_ALIGN, 64);
    REQUIRE(big);
    CHECK_EQ(countBlocks(base), 2);
    CHECK(allZero(big, 4096));
    const BaseBlock * newest = base->blocksList();
    CHECK(
        reinterpret_cast<uintptr_t>(big) >= reinterpret_cast<uintptr_t>(newest)
        && reinterpret_cast<uintptr_t>(big) < reinterpret_cast<uintptr_t>(newest) + newest->size);
    Stats s4 = getStats(base);
    CHECK_EQ(s4.mapped, BASE_BLOCK_MIN_ALIGN + newest->size);

    base->destroy(nullptr);
}

/// The size of an allocation that consumes most of the remaining space of the newest block (the avail heaps are
/// indexed by the floor size class, so only up to that size can be found), or 0 if less than 112 bytes are left.
size_t consumeSize(const Base * base)
{
    size_t remainder = base->blocksList()->extent.baseSize();
    if (remainder < 112)
        return 0;
    size_t floor_class = index_to_size_table[size_classes::sizeToIndex(remainder + 1) - 1];
    return floor_class - 16;
}

TEST(Base, BlockSizeSeries)
{
    bootOnce();
    Base * base = Base::create(nullptr, 2, &extent_hooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    /// Exhaust the newest block, then a small allocation maps the next block of the series.
    constexpr int n = 16;
    size_t sizes[n];
    sizes[0] = base->blocksList()->size;
    for (int i = 1; i < n; ++i)
    {
        while (size_t size = consumeSize(base))
            REQUIRE(base->alloc(nullptr, size, 16));
        REQUIRE(base->alloc(nullptr, 64, 64));
        sizes[i] = base->blocksList()->size;
    }
    std::fprintf(stderr, "block sizes:");
    for (size_t size : sizes)
        std::fprintf(stderr, " %zu", size >> 20);
    std::fprintf(stderr, " MiB\n");
    if constexpr (LOG2_PAGE == 12)
    {
        /// The next page size class above the previous block, rounded up to 2 MiB (spec 03, section 10.2).
        static constexpr size_t expected[n] = {2, 4, 6, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64};
        for (int i = 0; i < n; ++i)
            CHECK_EQ(sizes[i], expected[i] << 20);
    }
    for (int i = 1; i < n; ++i)
    {
        CHECK_EQ(sizes[i] % BASE_BLOCK_MIN_ALIGN, 0u);
        CHECK_GE(sizes[i], sizes[i - 1]);
    }
    base->destroy(nullptr);
}

TEST(Base, B0ThreadCacheStacks)
{
    bootOnce();
    REQUIRE(!baseBoot(nullptr));
    Base * b0 = base0Get();
    REQUIRE(b0 != nullptr);
    CHECK_EQ(b0->idxGet(), 0u);

    Stats s0 = getStats(b0);
    size_t stack_size = 1000;
    char * p = static_cast<char *>(b0AllocThreadCacheStack(nullptr, stack_size));
    REQUIRE(p);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p) % QUANTUM, 0u);
    CHECK(allZero(p, stack_size));
    Stats s1 = getStats(b0);
    /// An `Extent` for the piece (from `extent_available` if possible, else allocated) plus `sizeToUsableSize(size + 16)` bytes.
    CHECK_EQ(
        s1.allocated - s0.allocated, alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT) + size_classes::sizeToUsableSize(stack_size + 16));
    memset(p, 0x5a, stack_size);

    /// Freed stacks are zeroed and go back to the avail heaps; reused space does not count in the stats again.
    b0DeallocateThreadCacheStack(nullptr, p);
    char * q = static_cast<char *>(b0AllocThreadCacheStack(nullptr, stack_size));
    REQUIRE(q);
    CHECK(allZero(q, stack_size));
    Stats s2 = getStats(b0);
    size_t extent_size = alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT);
    if (size_classes::sizeToIndex(extent_size + EXTENT_ALIGNMENT - QUANTUM)
        <= size_classes::sizeToIndex(size_classes::sizeToUsableSize(stack_size + 16)))
    {
        /// The new `Extent` is carved from the freed piece (found first by the size class search), so the stack
        /// itself is bump-allocated anew (LOG2_PAGE = 12: `sizeof(Extent)` is 128).
        CHECK(q != p);
        CHECK_EQ(s2.allocated, s1.allocated + size_classes::sizeToUsableSize(stack_size + 16));
    }
    else
    {
        /// The new `Extent` comes from the block, the stack reuses the freed piece.
        CHECK(q == p);
        CHECK_EQ(s2.allocated, s1.allocated + extent_size);
    }
    b0DeallocateThreadCacheStack(nullptr, q);
}

TEST(ExtentPool, GetPut)
{
    bootOnce();
    Base * base = Base::create(nullptr, 5, &extent_hooks_default_extent_hooks, true);
    REQUIRE(base != nullptr);
    ExtentPool pool;
    REQUIRE(!pool.init(base));
    CHECK_EQ(pool.count(), 0u);

    Stats s0 = getStats(base);
    Extent * extents[8];
    for (auto & e : extents)
    {
        e = pool.get(nullptr);
        REQUIRE(e);
    }
    CHECK_EQ(getStats(base).extent_allocated - s0.extent_allocated, 8 * alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT));
    CHECK_EQ(pool.count(), 0u);

    /// Put back in reverse order; `get` returns the lowest (esn, address) first (all have esn 0 here).
    for (int i = 7; i >= 0; --i)
        pool.put(nullptr, extents[i]);
    CHECK_EQ(pool.count(), 8u);
    for (int i = 0; i < 8; ++i)
    {
        Extent * e = pool.get(nullptr);
        CHECK(e == extents[i]);
    }
    CHECK_EQ(pool.count(), 0u);
    /// The pool is empty again: a new structure comes from the base.
    Stats s1 = getStats(base);
    Extent * e = pool.get(nullptr);
    REQUIRE(e);
    CHECK_EQ(getStats(base).extent_allocated, s1.extent_allocated + alignmentCeiling(sizeof(Extent), EXTENT_ALIGNMENT));

    pool.prefork(nullptr);
    pool.postforkParent(nullptr);
    base->destroy(nullptr);
}
