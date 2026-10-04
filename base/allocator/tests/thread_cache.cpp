/// The thread cache: the `ncached_max` tables per page size, the boot globals, the `tcache_ncached_max` overrides,
/// the fill count adaptation (`cache_bin_fill_ctl_t`), the GC locality heuristic (remote pointer counting and the
/// bin shuffle) on synthetic bins.
/// Ported from jemalloc's `test/unit/ncached_max.c`, `test/unit/tcache_max.c` (the parts that do not need the
/// mallctl front-end) and the formulas of `tcache.c`.

#include <allocator/Conf.h>
#include <allocator/Options.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>

#include "Test.h"

#include <cstdlib>
#include <cstring>
#include <vector>

using namespace jemalloc;
using namespace jemalloc::tcache_detail;

namespace
{

/// Restores the tcache options after a test changes them.
struct OptionsGuard
{
    Options saved = opt;
    ~OptionsGuard()
    {
        opt = saved;
        tcacheBoot(nullptr, nullptr);
    }
};

/// An independent model of `tcache_ncached_max_compute` (spec 02, section 4.1).
unsigned modelNcachedMax(szind_t ind)
{
    if (ind >= SC_NBINS)
        return opt.tcache_nslots_large;
    unsigned lo = opt.tcache_nslots_small_min;
    unsigned hi = opt.tcache_nslots_small_max > 8191 ? 8191 : opt.tcache_nslots_small_max;
    lo += lo & 1;
    hi -= hi & 1;
    lo = lo < 2 ? 2 : lo;
    hi = hi < 2 ? 2 : hi;
    lo = lo > hi ? hi : lo;
    unsigned nregs = bin_infos[ind].nregs;
    unsigned c = opt.lg_tcache_nslots_mul < 0 ? (nregs >> -opt.lg_tcache_nslots_mul) : (nregs << opt.lg_tcache_nslots_mul);
    c += c & 1;
    return c <= lo ? lo : (c <= hi ? c : hi);
}

/// A cache bin with its own stack (as laid out by `tcache_init` for a single bin).
struct SyntheticBin
{
    CacheBin bin;
    void * mem = nullptr;

    explicit SyntheticBin(cache_bin_sz_t ncached_max)
    {
        CacheBinInfo info;
        info.init(ncached_max);
        size_t size;
        size_t alignment;
        cacheBinInfoComputeAlloc(&info, 1, size, alignment);
        mem = std::aligned_alloc(alignment, alignmentCeiling(size, alignment));
        size_t cur_offset = 0;
        cacheBinPreincrement(&info, 1, mem, cur_offset);
        bin.init(info, mem, cur_offset);
        cacheBinPostincrement(mem, cur_offset);
        REQUIRE(cur_offset == size);
    }

    ~SyntheticBin() { std::free(mem); }

    /// Pushes so that `items[0]` ends up at the head (top) of the stack.
    void fillTopFirst(const std::vector<uintptr_t> & items)
    {
        for (size_t i = items.size(); i > 0; --i)
            REQUIRE(bin.dallocEasy(reinterpret_cast<void *>(items[i - 1])));
    }

    std::vector<uintptr_t> contents() const
    {
        std::vector<uintptr_t> res;
        for (cache_bin_sz_t i = 0; i < bin.ncachedGetLocal(); ++i)
            res.push_back(reinterpret_cast<uintptr_t>(bin.stack_head[i]));
        return res;
    }
};

}

TEST(ThreadCache, Constants)
{
    /// tcache_types.h; spec 02, section 1.
    if constexpr (LG_PAGE == 12)
    {
        CHECK_EQ(TCACHE_NBINS_MAX, 41u);
        CHECK_EQ(TCACHE_MAXCLASS_LIMIT, size_t(32) << 10);
        CHECK_EQ(TCACHE_GC_SMALL_NBINS_MAX, 4u);
    }
    else if constexpr (LG_PAGE == 14)
    {
        CHECK_EQ(TCACHE_NBINS_MAX, 49u);
        CHECK_EQ(TCACHE_MAXCLASS_LIMIT, size_t(128) << 10);
        CHECK_EQ(TCACHE_GC_SMALL_NBINS_MAX, 5u);
    }
    else if constexpr (LG_PAGE == 16)
    {
        CHECK_EQ(TCACHE_NBINS_MAX, 57u);
        CHECK_EQ(TCACHE_MAXCLASS_LIMIT, size_t(512) << 10);
        CHECK_EQ(TCACHE_GC_SMALL_NBINS_MAX, 6u);
    }
    CHECK_EQ(TCACHE_GC_LARGE_NBINS_MAX, 1u);
    CHECK_EQ(TCACHE_GC_NEIGHBOR_LIMIT, uintptr_t(2) << 20);
    CHECK_EQ(TCACHE_GC_INTERVAL_NS, uint64_t(10000000));
    CHECK_EQ(MALLOCX_TCACHE_MAX, 4093u);
    CHECK_EQ(sizeof(ThreadCache), 8 + 24 * size_t(TCACHE_NBINS_MAX));
    CHECK_EQ(reinterpret_cast<uintptr_t>(TCACHES_ELM_NEED_REINIT), uintptr_t(1));
}

TEST(ThreadCache, BootDefaults)
{
    OptionsGuard guard;
    REQUIRE(!tcacheBoot(nullptr, nullptr));
    /// `arenas.tcache_max` and `arenas.nhbins` are 32 KiB and 41 for every page size.
    CHECK_EQ(global_do_not_change_tcache_maxclass, size_t(32768));
    CHECK_EQ(global_do_not_change_tcache_nbins, 41u);

    /// The default `ncached_max` table for 4 KiB pages (spec 02, section 4.1).
    if constexpr (LG_PAGE == 12)
    {
        const unsigned expected[41] = {200, 200, 200, 200, 128, 200, 200, 200, 64, 200, 128, 200, 32, 128,
                                       64,  128, 20,  64,  32,  64,  20,  32,  20, 32,  20,  20,  20, 20,
                                       20,  20,  20,  20,  20,  20,  20,  20,  20, 20,  20,  20,  20};
        for (szind_t i = 0; i < 41; ++i)
            CHECK_EQ(unsigned(tcacheGetDefaultNcachedMax()[i].ncached_max), expected[i]);
    }
    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        CHECK_EQ(unsigned(tcacheGetDefaultNcachedMax()[i].ncached_max), modelNcachedMax(i));
        CHECK(!tcacheGetDefaultNcachedMaxSet(i));
    }

    /// The default stack sizes (spec 02, section 2.4).
    size_t size;
    size_t alignment;
    cacheBinInfoComputeAlloc(tcacheGetDefaultNcachedMax(), global_do_not_change_tcache_nbins, size, alignment);
    CHECK_EQ(alignment, PAGE);
    if constexpr (LG_PAGE == 12)
        CHECK_EQ(size, size_t(24784));
    else if constexpr (LG_PAGE == 14)
        CHECK_EQ(size, size_t(36304));
    else if constexpr (LG_PAGE == 16)
        CHECK_EQ(size, size_t(47824));
}

TEST(ThreadCache, BootTcacheMax)
{
    OptionsGuard guard;
    /// `tcache_max:4096` (`test/unit/ncached_max.c`): the cached size classes end at 4096.
    opt.tcache_max = 4096;
    REQUIRE(!tcacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_tcache_maxclass, size_t(4096));
    CHECK_EQ(global_do_not_change_tcache_nbins, sz::sizeToIndex(4096) + 1);

    /// A non-class size is rounded up.
    opt.tcache_max = 5000;
    REQUIRE(!tcacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_tcache_maxclass, size_t(5120));
    CHECK_EQ(global_do_not_change_tcache_nbins, sz::sizeToIndex(5120) + 1);

    opt.tcache_max = TCACHE_MAXCLASS_LIMIT;
    REQUIRE(!tcacheBoot(nullptr, nullptr));
    CHECK_EQ(global_do_not_change_tcache_maxclass, TCACHE_MAXCLASS_LIMIT);
    CHECK_EQ(global_do_not_change_tcache_nbins, TCACHE_NBINS_MAX);
}

TEST(ThreadCache, NcachedMaxOptions)
{
    OptionsGuard guard;
    struct Case
    {
        ssize_t lg_mul;
        unsigned small_min;
        unsigned small_max;
        unsigned large;
    };
    const Case cases[] = {
        {1, 20, 200, 20},
        {0, 20, 200, 20},
        {-1, 20, 200, 20},
        {-16, 20, 200, 20},
        {2, 1, 2048, 7},
        {16, 21, 201, 1},
        {1, 300, 100, 20}, /// min > max: min becomes max.
        {1, 1, 1, 20}, /// Both clamped to 2.
        {3, 2048, 2048, 2048},
    };
    for (const auto & c : cases)
    {
        opt.lg_tcache_nslots_mul = c.lg_mul;
        opt.tcache_nslots_small_min = c.small_min;
        opt.tcache_nslots_small_max = c.small_max;
        opt.tcache_nslots_large = c.large;
        CacheBinInfo infos[TCACHE_NBINS_MAX];
        tcacheBinInfoCompute(infos);
        for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
        {
            CHECK_EQ(tcacheNcachedMaxCompute(i), modelNcachedMax(i));
            CHECK_EQ(unsigned(infos[i].ncached_max), modelNcachedMax(i));
        }
    }

    /// Pinned values: the 8-byte class has the most regions per slab, the large classes use `tcache_nslots_large`.
    opt.lg_tcache_nslots_mul = 1;
    opt.tcache_nslots_small_min = 21;
    opt.tcache_nslots_small_max = 201;
    opt.tcache_nslots_large = 7;
    CHECK_EQ(tcacheNcachedMaxCompute(0), 200u);
    CHECK_EQ(tcacheNcachedMaxCompute(SC_NBINS), 7u);
    /// The largest small class (one or two regions per slab) gets the clamped minimum.
    CHECK_EQ(tcacheNcachedMaxCompute(SC_NBINS - 1), 22u);
}

TEST(ThreadCache, NcachedMaxConf)
{
    OptionsGuard guard;
    /// The malloc_conf of `test/unit/ncached_max.c`.
    const char * conf = "256-1024:1001|2048-2048:0|8192-8192:1";
    REQUIRE(!tcacheBinInfoDefaultInit(conf, strlen(conf)));
    opt.tcache_max = 4096;
    REQUIRE(!tcacheBoot(nullptr, nullptr));

    CacheBinInfo infos[TCACHE_NBINS_MAX];
    tcacheBinInfoCompute(infos);
    for (szind_t i = 0; i < TCACHE_NBINS_MAX; ++i)
    {
        bool first_range = (i >= sz::sizeToIndex(256) && i <= sz::sizeToIndex(1024));
        bool second_range = (i == sz::sizeToIndex(2048));
        bool third_range = (i == sz::sizeToIndex(8192));
        if (first_range || second_range || third_range)
        {
            unsigned target = first_range ? 1001 : (second_range ? 0 : 1);
            CHECK(tcacheGetDefaultNcachedMaxSet(i));
            CHECK_EQ(unsigned(infos[i].ncached_max), target);
            CHECK_EQ(unsigned(tcacheGetDefaultNcachedMax()[i].ncached_max), target);
        }
        else
        {
            CHECK(!tcacheGetDefaultNcachedMaxSet(i));
            CHECK_EQ(unsigned(infos[i].ncached_max), modelNcachedMax(i));
        }
    }

    /// Clipping: `n` above `CACHE_BIN_NCACHED_MAX`, a range end beyond the limit, an empty range.
    const char * conf2 = "8-8:100000|16384-100000000:3|64-32:5";
    REQUIRE(!tcacheBinInfoDefaultInit(conf2, strlen(conf2)));
    tcacheBinInfoCompute(infos);
    CHECK_EQ(unsigned(infos[0].ncached_max), 8191u);
    for (szind_t i = sz::sizeToIndex(16384); i < TCACHE_NBINS_MAX; ++i)
        CHECK_EQ(unsigned(infos[i].ncached_max), 3u);
    CHECK_EQ(unsigned(infos[sz::sizeToIndex(64)].ncached_max), modelNcachedMax(sz::sizeToIndex(64)));

    /// Malformed settings are rejected.
    const char * bad = "8-16";
    CHECK(tcacheBinInfoDefaultInit(bad, strlen(bad)));
}

TEST(ThreadCache, FillCountAdaptation)
{
    OptionsGuard guard;
    ThreadCacheSlow slow;
    const szind_t ind = 0;
    tcacheBinFillCtlInit(&slow, ind);
    CacheBinFillCtl * ctl = tcacheBinFillCtlGet(&slow, ind);
    CHECK_EQ(ctl->base, 1);
    CHECK_EQ(ctl->offset, 0);
    CHECK_EQ(tcacheNfillSmallLgDivGet(&slow, ind), 1);

    /// No burst room at base 1.
    tcacheNfillSmallBurstPrepare(&slow, ind);
    CHECK_EQ(ctl->offset, 0);

    /// GC periods with unused items halve the fill count while `ncached_max >> base > 1`: for 200 the base stops at 7
    /// (200 >> 7 == 1), i.e. fills of 100, 50, 25, 12, 6, 3, 1.
    const unsigned expected_nfill[] = {100, 50, 25, 12, 6, 3, 1, 1, 1};
    for (unsigned step = 0; step < 9; ++step)
    {
        unsigned nfill = 200u >> tcacheNfillSmallLgDivGet(&slow, ind);
        CHECK_EQ(nfill == 0 ? 1u : nfill, expected_nfill[step]);
        tcacheNfillSmallGcUpdate(&slow, ind, 200);
    }
    CHECK_EQ(ctl->base, 7);

    /// Bursts within a GC period: the offset grows up to base - 1.
    for (unsigned i = 1; i <= 10; ++i)
    {
        tcacheNfillSmallBurstPrepare(&slow, ind);
        CHECK_EQ(unsigned(ctl->offset), i < 6 ? i : 6u);
        CHECK_EQ(unsigned(tcacheNfillSmallLgDivGet(&slow, ind)), 7u - (i < 6 ? i : 6u));
    }
    /// A flush resets the offset.
    tcacheNfillSmallBurstReset(&slow, ind);
    CHECK_EQ(ctl->offset, 0);

    /// Without the experimental GC the offset is ignored.
    tcacheNfillSmallBurstPrepare(&slow, ind);
    tcacheNfillSmallBurstPrepare(&slow, ind);
    opt.experimental_tcache_gc = false;
    CHECK_EQ(tcacheNfillSmallLgDivGet(&slow, ind), 7);
    opt.experimental_tcache_gc = true;
    CHECK_EQ(tcacheNfillSmallLgDivGet(&slow, ind), 5);

    /// A GC update resets the offset; periods with refills and no unused items double the fill count down to base 1.
    for (unsigned expected_base = 6; expected_base >= 1; --expected_base)
    {
        tcacheNfillSmallGcUpdate(&slow, ind, 0);
        CHECK_EQ(unsigned(ctl->base), expected_base);
        CHECK_EQ(ctl->offset, 0);
    }
    tcacheNfillSmallGcUpdate(&slow, ind, 0);
    CHECK_EQ(ctl->base, 1);

    /// Small `ncached_max`: 2 >> 1 == 1, so the base never grows.
    tcacheNfillSmallGcUpdate(&slow, ind, 2);
    CHECK_EQ(ctl->base, 1);
    tcacheNfillSmallGcUpdate(&slow, ind, 4);
    CHECK_EQ(ctl->base, 2);
    tcacheNfillSmallGcUpdate(&slow, ind, 4);
    CHECK_EQ(ctl->base, 2);
}

TEST(ThreadCache, GcItemDelay)
{
    OptionsGuard guard;
    opt.tcache_gc_delay_bytes = 0;
    CHECK_EQ(tcacheGcItemDelayCompute(0), 0);
    opt.tcache_gc_delay_bytes = 1024;
    CHECK_EQ(tcacheGcItemDelayCompute(0), 128); /// 8-byte class
    CHECK_EQ(tcacheGcItemDelayCompute(1), 64);
    CHECK_EQ(tcacheGcItemDelayCompute(sz::sizeToIndex(1024)), 1);
    CHECK_EQ(tcacheGcItemDelayCompute(sz::sizeToIndex(2048)), 0);
    opt.tcache_gc_delay_bytes = 1 << 20;
    CHECK_EQ(tcacheGcItemDelayCompute(0), 255);
    CHECK_EQ(tcacheGcItemDelayCompute(sz::sizeToIndex(4096)), 255);
    CHECK_EQ(tcacheGcItemDelayCompute(sz::sizeToIndex(8192)), 128);
}

TEST(ThreadCache, GcShuffle)
{
    /// The hand-computed example: (head -> bottom) [R1, L1, R2, L2, L3, R3] becomes [L1, L2, L3, R1, R2, R3].
    const uintptr_t lo = 0x100000;
    const uintptr_t hi = 0x200000;
    const uintptr_t L1 = lo + 0x10;
    const uintptr_t L2 = lo + 0x20;
    const uintptr_t L3 = lo + 0x30;
    const uintptr_t R1 = hi + 0x10;
    const uintptr_t R2 = 0x10;
    const uintptr_t R3 = hi + 0x30;
    {
        SyntheticBin sb(20);
        sb.fillTopFirst({R1, L1, R2, L2, L3, R3});
        tcacheGcSmallBinShuffle(&sb.bin, 3, lo, hi);
        CHECK(sb.contents() == (std::vector<uintptr_t>{L1, L2, L3, R1, R2, R3}));
    }
    {
        /// Remote items only in the top part: the bottom (remote-free) part is swapped up.
        SyntheticBin sb(20);
        sb.fillTopFirst({R1, R2, L1, L2, L3});
        tcacheGcSmallBinShuffle(&sb.bin, 2, lo, hi);
        CHECK(sb.contents() == (std::vector<uintptr_t>{L1, L2, L3, R2, R1}));
    }
    {
        /// Exhaustive: every remote/local pattern of 8 items keeps the local items in order on top.
        for (unsigned mask = 1; mask < 255; ++mask)
        {
            std::vector<uintptr_t> items;
            std::vector<uintptr_t> locals;
            unsigned nremote = 0;
            for (unsigned i = 0; i < 8; ++i)
            {
                if (mask & (1u << i))
                {
                    items.push_back(hi + 0x1000 * (i + 1));
                    ++nremote;
                }
                else
                {
                    items.push_back(lo + 0x100 * (i + 1));
                    locals.push_back(items.back());
                }
            }
            SyntheticBin sb(20);
            sb.fillTopFirst(items);
            tcacheGcSmallBinShuffle(&sb.bin, static_cast<cache_bin_sz_t>(nremote), lo, hi);
            auto res = sb.contents();
            CHECK(std::vector<uintptr_t>(res.begin(), res.begin() + long(locals.size())) == locals);
            for (size_t i = locals.size(); i < res.size(); ++i)
                CHECK(res[i] >= hi);
        }
    }
}

TEST(ThreadCache, GcNremote)
{
    const szind_t szind = 0;
    const size_t slab_size = bin_infos[szind].slab_size;
    const uintptr_t addr = uintptr_t(1) << 32;
    const uintptr_t two_mib = uintptr_t(2) << 20;

    SyntheticBin sb(200);
    /// 3 in the slab, 2 in the neighborhood (outside the slab), 4 far away.
    std::vector<uintptr_t> items = {
        addr, addr + slab_size - 8, addr + 16, addr + slab_size, addr - 8, addr + two_mib, addr - two_mib - 8, 0x1000,
        addr + (uintptr_t(1) << 30)};
    sb.fillTopFirst(items);

    uintptr_t min;
    uintptr_t max;
    /// nflush <= number of far pointers: keep the neighborhood.
    CHECK_EQ(unsigned(tcacheGcSmallNremoteGet(&sb.bin, reinterpret_cast<void *>(addr), min, max, szind, 4)), 4u);
    CHECK_EQ(min, addr - two_mib);
    CHECK_EQ(max, addr + two_mib);
    CHECK_EQ(unsigned(tcacheGcSmallNremoteGet(&sb.bin, reinterpret_cast<void *>(addr), min, max, szind, 0)), 4u);
    /// More to flush than far pointers: keep only the slab.
    CHECK_EQ(unsigned(tcacheGcSmallNremoteGet(&sb.bin, reinterpret_cast<void *>(addr), min, max, szind, 5)), 6u);
    CHECK_EQ(min, addr);
    CHECK_EQ(max, addr + slab_size);

    /// Near the bottom of the address space the neighborhood starts at 0.
    CHECK_EQ(unsigned(tcacheGcSmallNremoteGet(&sb.bin, reinterpret_cast<void *>(uintptr_t(0x100000)), min, max, szind, 0)), 8u);
    CHECK_EQ(min, uintptr_t(0));
    CHECK_EQ(max, uintptr_t(0x100000) + two_mib);

    /// Remote checks are half-open intervals.
    CHECK(!tcacheGcIsAddrRemote(reinterpret_cast<void *>(uintptr_t(10)), 10, 20));
    CHECK(tcacheGcIsAddrRemote(reinterpret_cast<void *>(uintptr_t(20)), 10, 20));
    CHECK(tcacheGcIsAddrRemote(reinterpret_cast<void *>(uintptr_t(9)), 10, 20));
}
