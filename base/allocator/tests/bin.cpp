/// Unit tests of `Bin` (`bin.c`, `bin_inlines.h`) on synthetic slabs (extents with fake addresses; the bin never
/// touches the region memory): region allocation order (single and batch), the slab selection order (`slabcur`, the
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

Extent * makeSlab(szind_t binind, uint64_t sn, unsigned slab_index)
{
    static_assert(alignof(Extent) <= EDATA_ALIGNMENT);
    auto * e = static_cast<Extent *>(std::aligned_alloc(EDATA_ALIGNMENT, (sizeof(Extent) + EDATA_ALIGNMENT - 1) / EDATA_ALIGNMENT * EDATA_ALIGNMENT));
    std::memset(static_cast<void *>(e), 0, sizeof(Extent));
    const BinInfo & info = bin_infos[binind];
    void * addr = reinterpret_cast<void *>(FAKE_BASE + uintptr_t(slab_index) * (info.slab_size + PAGE));
    e->init(1, addr, info.slab_size, true, binind, sn, extent_state_active, false, true, EXTENT_PAI_PAC, EXTENT_NOT_HEAD);
    e->setNfreeBinshard(info.nregs, 0);
    bitmapInit(e->slabData()->bitmap, info.bitmap_info, false);
    return e;
}

void bootDivInfo()
{
    for (unsigned i = 0; i < SC_NBINS; ++i)
        arena_binind_div_info[i].init(bin_infos[i].reg_size);
}

uintptr_t regAddr(const Extent * slab, szind_t binind, size_t regind)
{
    return reinterpret_cast<uintptr_t>(slab->addr()) + regind * bin_infos[binind].reg_size;
}

}

TEST(Bin, Init)
{
    Bin bin;
    CHECK(!bin.init());
    CHECK(bin.slabcur == nullptr);
    CHECK(bin.slabs_nonfull.empty());
    CHECK(bin.slabs_full.empty());
    CHECK_EQ(bin.stats.nmalloc, 0u);
}

TEST(Bin, SlabRegAllocOrder)
{
    for (szind_t binind : {szind_t(0), szind_t(5), szind_t(SC_NBINS - 1)})
    {
        const BinInfo & info = bin_infos[binind];
        Extent * slab = makeSlab(binind, 1, 0);
        for (unsigned i = 0; i < info.nregs; ++i)
        {
            CHECK_EQ(reinterpret_cast<uintptr_t>(Bin::slabRegAlloc(slab, info)), regAddr(slab, binind, i));
            CHECK_EQ(slab->nfree(), info.nregs - i - 1);
        }
    }
}

TEST(Bin, SlabRegAllocBatchMatchesSingle)
{
    for (szind_t binind = 0; binind < SC_NBINS; ++binind)
    {
        const BinInfo & info = bin_infos[binind];
        Extent * a = makeSlab(binind, 1, 0);
        Extent * b = makeSlab(binind, 1, 0);
        /// Free a pattern of holes in both (allocate everything, then free every third region).
        std::vector<void *> all(info.nregs);
        Bin::slabRegAllocBatch(a, info, info.nregs, all.data());
        for (unsigned i = 0; i < info.nregs; ++i)
        {
            CHECK_EQ(reinterpret_cast<uintptr_t>(all[i]), regAddr(a, binind, i));
            Bin::slabRegAlloc(b, info);
        }
        for (unsigned i = 0; i < info.nregs; i += 3)
        {
            bitmapUnset(a->slabData()->bitmap, info.bitmap_info, i);
            a->nfreeInc();
            bitmapUnset(b->slabData()->bitmap, info.bitmap_info, i);
            b->nfreeInc();
        }
        unsigned nfree = a->nfree();
        unsigned cnt = nfree > 1 ? nfree - 1 : nfree;
        std::vector<void *> batch(cnt);
        Bin::slabRegAllocBatch(a, info, cnt, batch.data());
        for (unsigned i = 0; i < cnt; ++i)
            CHECK(batch[i] == Bin::slabRegAlloc(b, info));
        CHECK_EQ(a->nfree(), b->nfree());
    }
}

TEST(Bin, SlabSelection)
{
    bootDivInfo();
    const szind_t binind = 3;
    const BinInfo & info = bin_infos[binind];
    Bin bin;
    REQUIRE(!bin.init());
    bin.lock.lock(nullptr);
    /// `curregs` is maintained by the arena around the bin calls (`nmalloc`/`curregs` on allocation); only
    /// `dallocLockedFinish` updates it here.
    bin.stats.curregs = 1000000;

    /// Empty: no fresh slab -> null.
    CHECK(bin.mallocNoFreshSlab(nullptr, false, binind) == nullptr);

    Extent * s1 = makeSlab(binind, 10, 1);
    void * p = bin.mallocWithFreshSlab(nullptr, binind, s1);
    CHECK_EQ(reinterpret_cast<uintptr_t>(p), regAddr(s1, binind, 0));
    CHECK(bin.slabcur == s1);
    CHECK_EQ(bin.stats.nslabs, 1u);
    CHECK_EQ(bin.stats.curslabs, 1u);

    /// Use up s1: it goes to the full list (manual arena) when slabcur is refilled.
    std::vector<void *> s1_regs{p};
    while (s1->nfree() > 0)
        s1_regs.push_back(bin.mallocNoFreshSlab(nullptr, false, binind));
    CHECK(bin.mallocNoFreshSlab(nullptr, false, binind) == nullptr);
    CHECK(bin.slabcur == nullptr);
    CHECK(bin.slabs_full.first() == s1);

    /// Two more slabs: s3 is older (smaller sn) than s2.
    Extent * s2 = makeSlab(binind, 30, 2);
    Extent * s3 = makeSlab(binind, 20, 3);
    bin.mallocWithFreshSlab(nullptr, binind, s2);
    bin.lowerSlab(nullptr, false, s3);
    /// `lowerSlab` switches slabcur to the older slab; the previous one goes to the non-full heap.
    CHECK(bin.slabcur == s3);
    CHECK(bin.slabs_nonfull.first() == s2);
    CHECK_EQ(bin.stats.reslabs, 1u);
    CHECK_EQ(bin.stats.nonfull_slabs, 1u);

    /// Freeing a region of the full slab s1 makes it non-full; it is older than slabcur (s3), so it becomes slabcur.
    BinDallocLockedInfo dinfo;
    Bin::dallocLockedBegin(dinfo, binind);
    CHECK(!bin.dallocLockedStep(nullptr, false, dinfo, binind, s1, s1_regs[7]));
    bin.dallocLockedFinish(nullptr, dinfo);
    CHECK(bin.slabcur == s1);
    CHECK(bin.slabs_full.empty());
    CHECK_EQ(bin.stats.ndalloc, 1u);
    /// The freed region is reused first.
    CHECK(bin.mallocNoFreshSlab(nullptr, false, binind) == s1_regs[7]);

    /// The non-full heap returns the lowest (sn, address) slab.
    CHECK(bin.slabs_nonfull.first() == s3 || bin.slabs_nonfull.first() == s2);
    Extent * first = bin.slabsNonfullTryget();
    CHECK(first == s3);

    /// Freeing every region of a slab reports it as empty (to be released) and dissociates it.
    Bin::dallocLockedBegin(dinfo, binind);
    size_t curslabs = bin.stats.curslabs;
    bool released = false;
    for (size_t i = 0; i < s1_regs.size(); ++i)
    {
        if (i == 7)
            continue;
        released = bin.dallocLockedStep(nullptr, false, dinfo, binind, s1, s1_regs[i]);
    }
    /// The last one frees region 7.
    released = bin.dallocLockedStep(nullptr, false, dinfo, binind, s1, s1_regs[7]);
    bin.dallocLockedFinish(nullptr, dinfo);
    CHECK(released);
    CHECK(bin.slabcur == nullptr);
    CHECK_EQ(bin.stats.curslabs, curslabs - 1);
    CHECK_EQ(s1->nfree(), info.nregs);
    bin.lock.unlock(nullptr);
}

TEST(Bin, AutoArenaSkipsFullList)
{
    bootDivInfo();
    const szind_t binind = 0;
    Bin bin;
    REQUIRE(!bin.init());
    bin.lock.lock(nullptr);
    Extent * s = makeSlab(binind, 1, 4);
    bin.mallocWithFreshSlab(nullptr, binind, s);
    while (s->nfree() > 0)
        bin.mallocNoFreshSlab(nullptr, true, binind);
    CHECK(bin.mallocNoFreshSlab(nullptr, true, binind) == nullptr);
    CHECK(bin.slabs_full.empty());
    bin.lock.unlock(nullptr);
}

TEST(Bin, StatsMerge)
{
    Bin bin;
    REQUIRE(!bin.init());
    bin.stats.nmalloc = 5;
    bin.stats.curregs = 3;
    bin.stats.nonfull_slabs = 2;
    BinStatsData data{};
    bin.statsMerge(nullptr, data);
    bin.statsMerge(nullptr, data);
    CHECK_EQ(data.stats_data.nmalloc, 10u);
    CHECK_EQ(data.stats_data.curregs, 6u);
    CHECK_EQ(data.stats_data.nonfull_slabs, 4u);
    /// The counters are read while holding the lock (the first merge sees 1 operation, the second 2).
    CHECK_EQ(data.mutex_data.n_lock_ops, 3u);
}
