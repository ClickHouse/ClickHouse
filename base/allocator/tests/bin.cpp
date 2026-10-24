/// Unit tests of `Bin` (`bin.c`, `bin_inlines.h`) on synthetic slabs (extents with fake addresses; the bin never
/// touches the region memory): region allocation order (single and batch), the slab selection order (`current_slab`, the
/// non-full heap ordered by (sn, address), the full list of manual arenas), the locked deallocation steps and the
/// stats counters.

#include <allocator/Arena.h>
#include <allocator/Bin.h>
#include <allocator/Bitmap.h>
#include <allocator/Extent.h>
#include <allocator/SizeClasses.h>

#include "Test.h"

#include <cstdlib>
#include <vector>

using namespace jemalloc;

namespace
{

/// Fake slab addresses (never dereferenced).
constexpr uintptr_t FAKE_BASE = uintptr_t(1) << 40;

Extent * makeSlab(SizeClassIdx bin_idx, uint64_t serial_number, unsigned slab_index)
{
    static_assert(alignof(Extent) <= EXTENT_ALIGNMENT);
    auto * e = static_cast<Extent *>(
        std::aligned_alloc(EXTENT_ALIGNMENT, (sizeof(Extent) + EXTENT_ALIGNMENT - 1) / EXTENT_ALIGNMENT * EXTENT_ALIGNMENT));
    std::memset(static_cast<void *>(e), 0, sizeof(Extent));
    const BinInfo & info = bin_infos[bin_idx];
    void * addr = reinterpret_cast<void *>(FAKE_BASE + uintptr_t(slab_index) * (info.slab_size + PAGE));
    e->init(
        1,
        addr,
        info.slab_size,
        true,
        bin_idx,
        serial_number,
        extent_state_active,
        false,
        true,
        EXTENT_ALLOCATOR_PAGE_ALLOCATOR,
        EXTENT_NOT_HEAD);
    e->setNumFreeBinShard(info.num_regions, 0);
    bitmapInit(e->slabData()->bitmap, info.bitmap_info, false);
    return e;
}

void bootDivisionInfo()
{
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        arena_bin_idx_division_info[i].init(bin_infos[i].region_size);
}

uintptr_t regionAddr(const Extent * slab, SizeClassIdx bin_idx, size_t region_idx)
{
    return reinterpret_cast<uintptr_t>(slab->addr()) + region_idx * bin_infos[bin_idx].region_size;
}

}

TEST(Bin, Init)
{
    Bin bin;
    CHECK(!bin.init());
    CHECK(bin.current_slab == nullptr);
    CHECK(bin.slabs_non_full.empty());
    CHECK(bin.slabs_full.empty());
    CHECK_EQ(bin.stats.num_allocations, 0u);
}

TEST(Bin, SlabRegionAllocOrder)
{
    for (SizeClassIdx bin_idx : {SizeClassIdx(0), SizeClassIdx(5), SizeClassIdx(SIZE_CLASS_NUM_BINS - 1)})
    {
        const BinInfo & info = bin_infos[bin_idx];
        Extent * slab = makeSlab(bin_idx, 1, 0);
        for (unsigned i = 0; i < info.num_regions; ++i)
        {
            CHECK_EQ(reinterpret_cast<uintptr_t>(Bin::slabRegionAlloc(slab, info)), regionAddr(slab, bin_idx, i));
            CHECK_EQ(slab->numFree(), info.num_regions - i - 1);
        }
    }
}

TEST(Bin, SlabRegionAllocBatchMatchesSingle)
{
    for (SizeClassIdx bin_idx = 0; bin_idx < SIZE_CLASS_NUM_BINS; ++bin_idx)
    {
        const BinInfo & info = bin_infos[bin_idx];
        Extent * a = makeSlab(bin_idx, 1, 0);
        Extent * b = makeSlab(bin_idx, 1, 0);
        /// Free a pattern of holes in both (allocate everything, then free every third region).
        std::vector<void *> all(info.num_regions);
        Bin::slabRegionAllocBatch(a, info, info.num_regions, all.data());
        for (unsigned i = 0; i < info.num_regions; ++i)
        {
            CHECK_EQ(reinterpret_cast<uintptr_t>(all[i]), regionAddr(a, bin_idx, i));
            Bin::slabRegionAlloc(b, info);
        }
        for (unsigned i = 0; i < info.num_regions; i += 3)
        {
            bitmapUnset(a->slabData()->bitmap, info.bitmap_info, i);
            a->numFreeIncrement();
            bitmapUnset(b->slabData()->bitmap, info.bitmap_info, i);
            b->numFreeIncrement();
        }
        unsigned num_free = a->numFree();
        unsigned count = num_free > 1 ? num_free - 1 : num_free;
        std::vector<void *> batch(count);
        Bin::slabRegionAllocBatch(a, info, count, batch.data());
        for (unsigned i = 0; i < count; ++i)
            CHECK(batch[i] == Bin::slabRegionAlloc(b, info));
        CHECK_EQ(a->numFree(), b->numFree());
    }
}

TEST(Bin, SlabSelection)
{
    bootDivisionInfo();
    const SizeClassIdx bin_idx = 3;
    const BinInfo & info = bin_infos[bin_idx];
    Bin bin;
    REQUIRE(!bin.init());
    bin.lock.lock(nullptr);
    /// `current_regions` is maintained by the arena around the bin calls (`num_allocations`/`current_regions` on allocation); only
    /// `deallocateLockedFinish` updates it here.
    bin.stats.current_regions = 1000000;

    /// Empty: no fresh slab -> null.
    CHECK(bin.mallocNoFreshSlab(nullptr, false, bin_idx) == nullptr);

    Extent * s1 = makeSlab(bin_idx, 10, 1);
    void * p = bin.mallocWithFreshSlab(nullptr, bin_idx, s1);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p), regionAddr(s1, bin_idx, 0));
    CHECK(bin.current_slab == s1);
    CHECK_EQ(bin.stats.num_slabs, 1u);
    CHECK_EQ(bin.stats.current_slabs, 1u);

    /// Use up s1: it goes to the full list (manual arena) when slabcur is refilled.
    std::vector<void *> s1_regions{p};
    while (s1->numFree() > 0)
        s1_regions.push_back(bin.mallocNoFreshSlab(nullptr, false, bin_idx));
    CHECK(bin.mallocNoFreshSlab(nullptr, false, bin_idx) == nullptr);
    CHECK(bin.current_slab == nullptr);
    CHECK(bin.slabs_full.first() == s1);

    /// Two more slabs: s3 is older (smaller sn) than s2.
    Extent * s2 = makeSlab(bin_idx, 30, 2);
    Extent * s3 = makeSlab(bin_idx, 20, 3);
    bin.mallocWithFreshSlab(nullptr, bin_idx, s2);
    bin.lowerSlab(nullptr, false, s3);
    /// `lowerSlab` switches slabcur to the older slab; the previous one goes to the non-full heap.
    CHECK(bin.current_slab == s3);
    CHECK(bin.slabs_non_full.first() == s2);
    CHECK_EQ(bin.stats.slab_changes, 1u);
    CHECK_EQ(bin.stats.non_full_slabs, 1u);

    /// Freeing a region of the full slab s1 makes it non-full; it is older than slabcur (s3), so it becomes slabcur.
    BinDeallocateLockedInfo deallocation_info;
    Bin::deallocateLockedBegin(deallocation_info, bin_idx);
    CHECK(!bin.deallocateLockedStep(nullptr, false, deallocation_info, bin_idx, s1, s1_regions[7]));
    bin.deallocateLockedFinish(nullptr, deallocation_info);
    CHECK(bin.current_slab == s1);
    CHECK(bin.slabs_full.empty());
    CHECK_EQ(bin.stats.num_deallocations, 1u);
    /// The freed region is reused first.
    CHECK(bin.mallocNoFreshSlab(nullptr, false, bin_idx) == s1_regions[7]);

    /// The non-full heap returns the lowest (sn, address) slab.
    CHECK(bin.slabs_non_full.first() == s3 || bin.slabs_non_full.first() == s2);
    Extent * first = bin.slabsNonFullTryGet();
    CHECK(first == s3);

    /// Freeing every region of a slab reports it as empty (to be released) and dissociates it.
    Bin::deallocateLockedBegin(deallocation_info, bin_idx);
    size_t current_slabs = bin.stats.current_slabs;
    bool released = false;
    for (size_t i = 0; i < s1_regions.size(); ++i)
    {
        if (i == 7)
            continue;
        released = bin.deallocateLockedStep(nullptr, false, deallocation_info, bin_idx, s1, s1_regions[i]);
    }
    /// The last one frees region 7.
    released = bin.deallocateLockedStep(nullptr, false, deallocation_info, bin_idx, s1, s1_regions[7]);
    bin.deallocateLockedFinish(nullptr, deallocation_info);
    CHECK(released);
    CHECK(bin.current_slab == nullptr);
    CHECK_EQ(bin.stats.current_slabs, current_slabs - 1);
    CHECK_EQ(s1->numFree(), info.num_regions);
    bin.lock.unlock(nullptr);
}

TEST(Bin, AutoArenaSkipsFullList)
{
    bootDivisionInfo();
    const SizeClassIdx bin_idx = 0;
    Bin bin;
    REQUIRE(!bin.init());
    bin.lock.lock(nullptr);
    Extent * s = makeSlab(bin_idx, 1, 4);
    bin.mallocWithFreshSlab(nullptr, bin_idx, s);
    while (s->numFree() > 0)
        bin.mallocNoFreshSlab(nullptr, true, bin_idx);
    CHECK(bin.mallocNoFreshSlab(nullptr, true, bin_idx) == nullptr);
    CHECK(bin.slabs_full.empty());
    bin.lock.unlock(nullptr);
}

TEST(Bin, StatsMerge)
{
    Bin bin;
    REQUIRE(!bin.init());
    bin.stats.num_allocations = 5;
    bin.stats.current_regions = 3;
    bin.stats.non_full_slabs = 2;
    BinStatsData data{};
    bin.statsMerge(nullptr, data);
    bin.statsMerge(nullptr, data);
    CHECK_EQ(data.stats_data.num_allocations, 10u);
    CHECK_EQ(data.stats_data.current_regions, 6u);
    CHECK_EQ(data.stats_data.non_full_slabs, 4u);
    /// The counters are read while holding the lock (the first merge sees 1 operation, the second 2).
    CHECK_EQ(data.mutex_data.num_lock_ops, 3u);
}
