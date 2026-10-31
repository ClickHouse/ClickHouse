/// Unit tests of the flat bitmap (`fb.h`) and `ExtentSet` (`eset.c`): exact search results, first-fit order by
/// (sn, addr), the fragmentation cap, the enumerate search of the floor bin, the alignment search, stats and LRU.
/// The extents are fake (addresses are never dereferenced). See extent_set_oracle.cpp for the randomized comparison
/// with jemalloc.

#include <allocator/ExtentSet.h>
#include <allocator/Options.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <vector>

using namespace jemalloc;

namespace
{

Extent * newExtent(uintptr_t addr, size_t size, uint64_t serial_number)
{
    void * p = std::aligned_alloc(EXTENT_ALIGNMENT, (sizeof(Extent) + EXTENT_ALIGNMENT - 1) / EXTENT_ALIGNMENT * EXTENT_ALIGNMENT);
    std::memset(p, 0, sizeof(Extent));
    Extent * e = static_cast<Extent *>(p);
    e->init(
        0,
        reinterpret_cast<void *>(addr),
        size,
        false,
        SIZE_CLASS_NUM_SIZES,
        serial_number,
        extent_state_dirty,
        false,
        true,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        EXTENT_NOT_HEAD);
    return e;
}

ExtentSet * newSet()
{
    void * p = std::aligned_alloc(64, (sizeof(ExtentSet) + 63) / 64 * 64);
    std::memset(p, 0, sizeof(ExtentSet));
    ExtentSet * s = static_cast<ExtentSet *>(p);
    s->init(extent_state_dirty);
    return s;
}

constexpr uintptr_t BASE_ADDR = uintptr_t(1) << 40;

}

TEST(FlatBitmap, Basics)
{
    FlatBitmap<200> flat_bitmap;
    flat_bitmap.init();
    CHECK(flat_bitmap.empty());
    CHECK(!flat_bitmap.full());
    CHECK_EQ(flat_bitmap.num_groups, size_t(4));
    CHECK_EQ(flat_bitmap.findFirstSet(0), size_t(200));
    CHECK_EQ(flat_bitmap.findFirstUnset(0), size_t(0));
    CHECK_EQ(flat_bitmap.findLastSet(199), ssize_t(-1));
    CHECK_EQ(flat_bitmap.findLastUnset(199), ssize_t(199));

    flat_bitmap.set(3);
    flat_bitmap.set(64);
    flat_bitmap.set(130);
    flat_bitmap.set(199);
    CHECK(flat_bitmap.get(3) && flat_bitmap.get(64) && flat_bitmap.get(130) && flat_bitmap.get(199) && !flat_bitmap.get(4));
    CHECK_EQ(flat_bitmap.findFirstSet(0), size_t(3));
    CHECK_EQ(flat_bitmap.findFirstSet(4), size_t(64));
    CHECK_EQ(flat_bitmap.findFirstSet(65), size_t(130));
    CHECK_EQ(flat_bitmap.findFirstSet(131), size_t(199));
    CHECK_EQ(flat_bitmap.findLastSet(198), ssize_t(130));
    CHECK_EQ(flat_bitmap.findLastSet(63), ssize_t(3));
    CHECK_EQ(flat_bitmap.findLastSet(2), ssize_t(-1));
    CHECK_EQ(flat_bitmap.setCount(0, 200), size_t(4));
    CHECK_EQ(flat_bitmap.unsetCount(0, 200), size_t(196));
    CHECK_EQ(flat_bitmap.setCount(4, 127), size_t(2));
    flat_bitmap.unset(64);
    CHECK_EQ(flat_bitmap.findFirstSet(4), size_t(130));

    /// Unset bits past the end of the last group are not reported.
    FlatBitmap<70> small;
    small.init();
    small.setRange(0, 70);
    CHECK(small.full());
    CHECK_EQ(small.findFirstUnset(0), size_t(70));
    small.unsetRange(10, 50);
    CHECK_EQ(small.findFirstUnset(0), size_t(10));
    CHECK_EQ(small.findFirstSet(10), size_t(60));
    CHECK_EQ(small.setCount(0, 70), size_t(20));
    CHECK_EQ(small.setRangeLongest(), size_t(10));
    CHECK_EQ(small.unsetRangeLongest(), size_t(50));

    size_t begin = 0;
    size_t len = 0;
    CHECK(small.setRangeIterate(0, &begin, &len));
    CHECK_EQ(begin, size_t(0));
    CHECK_EQ(len, size_t(10));
    CHECK(small.setRangeIterate(10, &begin, &len));
    CHECK_EQ(begin, size_t(60));
    CHECK_EQ(len, size_t(10));
    CHECK(small.unsetRangeIterate(0, &begin, &len));
    CHECK_EQ(begin, size_t(10));
    CHECK_EQ(len, size_t(50));
    CHECK(small.setRangeReverseIterate(69, &begin, &len));
    CHECK_EQ(begin, size_t(60));
    CHECK_EQ(len, size_t(10));
    CHECK(small.unsetRangeReverseIterate(69, &begin, &len));
    CHECK_EQ(begin, size_t(10));
    CHECK_EQ(len, size_t(50));
    CHECK(!small.unsetRangeIterate(60, &begin, &len));

    FlatBitmap<70> other;
    other.init();
    other.set(5);
    other.set(65);
    FlatBitmap<70> dst;
    FlatBitmap<70>::bitAnd(dst, small, other);
    CHECK_EQ(dst.setCount(0, 70), size_t(2));
    FlatBitmap<70>::bitOr(dst, small, other);
    CHECK_EQ(dst.setCount(0, 70), size_t(20));
    FlatBitmap<70>::bitNot(dst, small);
    CHECK_EQ(dst.setCount(0, 70), size_t(50));
}

TEST(ExtentSet, FirstFitOldestThenLowest)
{
    options.disable_large_size_classes = true;
    ExtentSet * s = newSet();
    /// Three 1-page extents and a large one; the fit takes the smallest (sn, addr) among the bins that fit.
    Extent * a = newExtent(BASE_ADDR + 10 * PAGE, PAGE, 5);
    Extent * b = newExtent(BASE_ADDR + 20 * PAGE, PAGE, 3);
    Extent * c = newExtent(BASE_ADDR + 5 * PAGE, PAGE, 3);
    Extent * big = newExtent(BASE_ADDR + 100 * PAGE, 4 * PAGE, 1);
    for (Extent * e : {a, b, c, big})
        s->insert(e);

    CHECK_EQ(s->numPagesGet(), size_t(7));
    CHECK_EQ(s->numExtentsGet(0), size_t(3));
    CHECK_EQ(s->numBytesGet(0), 3 * PAGE);
    CHECK_EQ(s->bins[0].heap_min.serial_number, uint64_t(3));
    CHECK_EQ(s->bins[0].heap_min.addr, BASE_ADDR + 5 * PAGE);

    /// The 4-page extent is older than the 1-page ones (the cap 2^6 allows it).
    CHECK(s->fit(PAGE, PAGE, false, 6) == big);
    /// With lg_max_fit = 0 the 4-page bin is beyond the cap.
    CHECK(s->fit(PAGE, PAGE, false, 0) == c);
    /// Exact fit: only the 1-page extents qualify; the enumeration picks the smallest summary.
    CHECK(s->fit(PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == c);
    CHECK(s->fit(4 * PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == big);
    CHECK(s->fit(2 * PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == nullptr);

    s->remove(c);
    CHECK_EQ(s->bins[0].heap_min.addr, BASE_ADDR + 20 * PAGE);
    CHECK(s->fit(PAGE, PAGE, false, 0) == b);
    /// LRU: insertion order without the removed extent.
    CHECK(s->lruFirst() == a);
    CHECK(s->lru.next(a) == b);
    CHECK(s->lru.next(b) == big);
    CHECK(s->lru.next(big) == nullptr);

    s->remove(a);
    s->remove(b);
    s->remove(big);
    CHECK(s->bitmap.empty());
    CHECK_EQ(s->numPagesGet(), size_t(0));
    CHECK(s->fit(PAGE, PAGE, false, SIZE_CLASS_PTR_BITS) == nullptr);
    for (Extent * e : {a, b, c, big})
        std::free(e);
    std::free(s);
}

TEST(ExtentSet, EnumerateFloorBin)
{
    /// Page size classes are 1..8, 10, 12, 14, 16, ... pages; with the cache-oblivious pad the bins hold "class + 1
    /// page", so a 12-page extent is in the bin of 11 pages (class 10 + pad), while a 12-page request starts the search
    /// at the bin of 13 pages (class 12 + pad): the floor bin is enumerated.
    options.disable_large_size_classes = true;
    ExtentSet * s = newSet();
    Extent * e12 = newExtent(BASE_ADDR, 12 * PAGE, 1);
    s->insert(e12);
    CHECK_EQ(size_classes::pageSizeQuantizeFloor(12 * PAGE), 11 * PAGE);
    CHECK_EQ(size_classes::pageSizeQuantizeCeil(12 * PAGE), 13 * PAGE);
    CHECK(s->bitmap.get(size_classes::pageSizeToPageSizeClassIdx(11 * PAGE)));
    CHECK(s->fit(12 * PAGE, PAGE, false, SIZE_CLASS_PTR_BITS) == e12);
    CHECK(s->fit(12 * PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == e12);
    CHECK(s->fit(11 * PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == nullptr);
    /// With large size classes enabled, only bins >= the ceiling are searched (exact fit: only the ceiling bin).
    options.disable_large_size_classes = false;
    CHECK(s->fit(12 * PAGE, PAGE, false, SIZE_CLASS_PTR_BITS) == nullptr);
    CHECK(s->fit(11 * PAGE, PAGE, true, SIZE_CLASS_PTR_BITS) == e12);
    options.disable_large_size_classes = true;
    /// Too big for the extent.
    CHECK(s->fit(13 * PAGE, PAGE, false, SIZE_CLASS_PTR_BITS) == nullptr);
    s->remove(e12);
    std::free(e12);
    std::free(s);
}

TEST(ExtentSet, AlignmentSearch)
{
    options.disable_large_size_classes = true;
    ExtentSet * s = newSet();
    /// A 3-page extent one page before a 4-page boundary: a 2-page request aligned to 4 pages fits after one page of
    /// lead, which the pessimistic `max_size` (2 + 4 - 1 = 5 pages) misses, so the alignment search finds it.
    uintptr_t boundary = BASE_ADDR + 64 * PAGE;
    Extent * e = newExtent(boundary - PAGE, 3 * PAGE, 1);
    s->insert(e);
    CHECK(s->fit(2 * PAGE, 4 * PAGE, false, SIZE_CLASS_PTR_BITS) == e);
    /// 3 pages aligned to 4 pages do not fit (only 2 pages after the boundary).
    CHECK(s->fit(3 * PAGE, 4 * PAGE, false, SIZE_CLASS_PTR_BITS) == nullptr);
    /// Overflow of `extent_size + alignment` returns null.
    CHECK(s->fit(SIZE_MAX - PAGE + 1, 2 * PAGE, false, SIZE_CLASS_PTR_BITS) == nullptr);
    s->remove(e);
    std::free(e);
    std::free(s);
}

TEST(ExtentSet, LastBin)
{
    options.disable_large_size_classes = true;
    ExtentSet * s = newSet();
    Extent * huge = newExtent(PAGE, SIZE_CLASS_LARGE_MAX_CLASS + PAGE, 1);
    s->insert(huge);
    CHECK(s->bitmap.get(SIZE_CLASS_NUM_PAGE_SIZES));
    CHECK_EQ(s->numExtentsGet(SIZE_CLASS_NUM_PAGE_SIZES), size_t(1));
    CHECK(s->fit(SIZE_CLASS_LARGE_MAX_CLASS, PAGE, false, SIZE_CLASS_PTR_BITS) == huge);
    s->remove(huge);
    std::free(huge);
    std::free(s);
}
