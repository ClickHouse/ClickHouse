/// Pins the size class tables to the values computed by jemalloc (`01-size-classes-bins.md` sections 1.1, 3, 4)
/// for the configured LG_PAGE (12, 14 or 16).

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <algorithm>

using namespace jemalloc;

namespace
{

constexpr size_t small_reg_sizes[] = {8, 16, 32, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640,
    768, 896, 1024, 1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384,
    20480, 24576, 28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, 163840, 196608, 229376};

/// lg_base / lg_delta / ndelta of classes 0..35 (identical for every page size).
constexpr int small_lg[36][3] = {{3, 3, 0}, {3, 3, 1}, {4, 4, 1}, {4, 4, 2}, {4, 4, 3}, {6, 4, 1}, {6, 4, 2}, {6, 4, 3},
    {6, 4, 4}, {7, 5, 1}, {7, 5, 2}, {7, 5, 3}, {7, 5, 4}, {8, 6, 1}, {8, 6, 2}, {8, 6, 3}, {8, 6, 4}, {9, 7, 1},
    {9, 7, 2}, {9, 7, 3}, {9, 7, 4}, {10, 8, 1}, {10, 8, 2}, {10, 8, 3}, {10, 8, 4}, {11, 9, 1}, {11, 9, 2},
    {11, 9, 3}, {11, 9, 4}, {12, 10, 1}, {12, 10, 2}, {12, 10, 3}, {12, 10, 4}, {13, 11, 1}, {13, 11, 2},
    {13, 11, 3}};

/// div_info_t.magic of classes 0..35 (identical for every page size).
constexpr uint32_t small_magic[36] = {536870912, 268435456, 134217728, 89478486, 67108864, 53687092, 44739243, 38347923,
    33554432, 26843546, 22369622, 19173962, 16777216, 13421773, 11184811, 9586981, 8388608, 6710887, 5592406, 4793491,
    4194304, 3355444, 2796203, 2396746, 2097152, 1677722, 1398102, 1198373, 1048576, 838861, 699051, 599187, 524288,
    419431, 349526, 299594};

constexpr unsigned pgs_12[] = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3,
    7, 2, 5, 3, 7};
constexpr unsigned nregs_12[] = {512, 256, 128, 256, 64, 256, 128, 256, 32, 128, 64, 128, 16, 64, 32, 64, 8, 32, 16, 32, 4,
    16, 8, 16, 2, 8, 4, 8, 1, 4, 2, 4, 1, 2, 1, 2};
constexpr unsigned groups_12[] = {8, 4, 2, 4, 1, 4, 2, 4, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1};

constexpr unsigned pgs_14[] = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3,
    7, 1, 5, 3, 7, 1, 5, 3, 7, 2, 5, 3, 7};
constexpr unsigned nregs_14[] = {2048, 1024, 512, 1024, 256, 1024, 512, 1024, 128, 512, 256, 512, 64, 256, 128, 256, 32,
    128, 64, 128, 16, 64, 32, 64, 8, 32, 16, 32, 4, 16, 8, 16, 2, 8, 4, 8, 1, 4, 2, 4, 1, 2, 1, 2};
/// levels:groups
constexpr unsigned bitmap_14[][2] = {{2, 33}, {2, 17}, {2, 9}, {2, 17}, {2, 5}, {2, 17}, {2, 9}, {2, 17}, {2, 3}, {2, 9},
    {2, 5}, {2, 9}, {1, 1}, {2, 5}, {2, 3}, {2, 5}, {1, 1}, {2, 3}, {1, 1}, {2, 3}};

constexpr unsigned pgs_16[] = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3,
    7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 2, 5, 3, 7};
constexpr unsigned nregs_16[] = {8192, 4096, 2048, 4096, 1024, 4096, 2048, 4096, 512, 2048, 1024, 2048, 256, 1024, 512,
    1024, 128, 512, 256, 512, 64, 256, 128, 256, 32, 128, 64, 128, 16, 64, 32, 64, 8, 32, 16, 32, 4, 16, 8, 16, 2, 8, 4,
    8, 1, 4, 2, 4, 1, 2, 1, 2};
constexpr unsigned bitmap_16[][2] = {{3, 131}, {2, 65}, {2, 33}, {2, 65}, {2, 17}, {2, 65}, {2, 33}, {2, 65}, {2, 9},
    {2, 33}, {2, 17}, {2, 33}, {2, 5}, {2, 17}, {2, 9}, {2, 17}, {2, 3}, {2, 9}, {2, 5}, {2, 9}, {1, 1}, {2, 5}, {2, 3},
    {2, 5}, {1, 1}, {2, 3}, {1, 1}, {2, 3}};

struct Expected
{
    unsigned nbins;
    unsigned npsizes;
    size_t small_maxclass;
    size_t large_minclass;
    unsigned usize_grow_slow_threshold;
    unsigned lg_slab_maxregs;
    unsigned lg_bitmap_maxbits;
    bool use_tree;
    size_t groups_max;
    const unsigned * pgs;
    const unsigned * nregs;
    /// For the tree bitmap: levels:groups for the first `nbitmap` bins, 1:1 for the rest. Null for the flat one.
    const unsigned (*bitmap)[2];
    size_t nbitmap;
};

constexpr Expected expected = []
{
    if constexpr (LG_PAGE == 12)
        return Expected{36, 199, 14336, 16384, 32768, 9, 9, false, 8, pgs_12, nregs_12, nullptr, 0};
    else if constexpr (LG_PAGE == 14)
        return Expected{44, 191, 57344, 65536, 131072, 11, 11, true, 33, pgs_14, nregs_14, bitmap_14, std::size(bitmap_14)};
    else
        return Expected{52, 183, 229376, 262144, 524288, 13, 13, true, 131, pgs_16, nregs_16, bitmap_16, std::size(bitmap_16)};
}();

void checkSameSizeClassData(const SizeClassData & a, const SizeClassData & b)
{
    CHECK_EQ(a.ntiny, b.ntiny);
    CHECK_EQ(a.nlbins, b.nlbins);
    CHECK_EQ(a.nbins, b.nbins);
    CHECK_EQ(a.nsizes, b.nsizes);
    CHECK_EQ(a.lg_ceil_nsizes, b.lg_ceil_nsizes);
    CHECK_EQ(a.npsizes, b.npsizes);
    CHECK_EQ(a.lg_tiny_maxclass, b.lg_tiny_maxclass);
    CHECK_EQ(a.lookup_maxclass, b.lookup_maxclass);
    CHECK_EQ(a.small_maxclass, b.small_maxclass);
    CHECK_EQ(a.lg_large_minclass, b.lg_large_minclass);
    CHECK_EQ(a.large_minclass, b.large_minclass);
    CHECK_EQ(a.large_maxclass, b.large_maxclass);
    CHECK_EQ(a.initialized, b.initialized);
    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        CHECK_EQ(a.sc[i].index, b.sc[i].index);
        CHECK_EQ(a.sc[i].lg_base, b.sc[i].lg_base);
        CHECK_EQ(a.sc[i].lg_delta, b.sc[i].lg_delta);
        CHECK_EQ(a.sc[i].ndelta, b.sc[i].ndelta);
        CHECK_EQ(a.sc[i].psz, b.sc[i].psz);
        CHECK_EQ(a.sc[i].bin, b.sc[i].bin);
        CHECK_EQ(a.sc[i].pgs, b.sc[i].pgs);
        CHECK_EQ(a.sc[i].lg_delta_lookup, b.sc[i].lg_delta_lookup);
    }
}

template <bool UseTree>
void checkSameBitmapInfo(const BitmapInfoImpl<UseTree> & a, const BitmapInfoImpl<UseTree> & b)
{
    CHECK_EQ(a.nbits, b.nbits);
    if constexpr (UseTree)
    {
        CHECK_EQ(a.nlevels, b.nlevels);
        for (unsigned l = 0; l <= BITMAP_MAX_LEVELS; ++l)
            CHECK_EQ(a.levels[l].group_offset, b.levels[l].group_offset);
    }
    else
        CHECK_EQ(a.ngroups, b.ngroups);
}

template <bool UseTree>
unsigned bitmapLevels(const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
        return info.nlevels;
    else
        return 1;
}

void checkSameBinInfo(const BinInfo & a, const BinInfo & b)
{
    CHECK_EQ(a.reg_size, b.reg_size);
    CHECK_EQ(a.slab_size, b.slab_size);
    CHECK_EQ(a.nregs, b.nregs);
    CHECK_EQ(a.n_shards, b.n_shards);
    checkSameBitmapInfo(a.bitmap_info, b.bitmap_info);
}

}

TEST(SizeClasses, Constants)
{
    CHECK_EQ(SC_NSIZES, 232u);
    CHECK_EQ(SC_NTINY, 1u);
    CHECK_EQ(SC_NBINS, expected.nbins);
    CHECK_EQ(SC_NSIZES - SC_NBINS, 232u - expected.nbins);
    CHECK_EQ(SC_NPSIZES, expected.npsizes);
    CHECK_EQ(SC_LOOKUP_MAXCLASS, 4096u);
    CHECK_EQ(SC_SMALL_MAXCLASS, expected.small_maxclass);
    CHECK_EQ(SC_LARGE_MINCLASS, expected.large_minclass);
    CHECK_EQ(SC_LARGE_MAXCLASS, size_t(0x7000000000000000ULL));
    CHECK_EQ(SC_LARGE_MAXCLASS, size_t(8070450532247928832ULL));
    CHECK_EQ(USIZE_GROW_SLOW_THRESHOLD, expected.usize_grow_slow_threshold);
    CHECK_EQ(SC_LG_SLAB_MAXREGS, expected.lg_slab_maxregs);
    CHECK_EQ(SC_SLAB_MAXREGS, 1u << expected.lg_slab_maxregs);
    CHECK_EQ(LG_BITMAP_MAXBITS, expected.lg_bitmap_maxbits);
    CHECK_EQ(BITMAP_USE_TREE, expected.use_tree);
    CHECK_EQ(BITMAP_GROUPS_MAX, expected.groups_max);
    CHECK_EQ(sizeof(BitmapInfo), expected.use_tree ? 64u : 16u);
    CHECK_EQ(sizeof(BinInfo), expected.use_tree ? 88u : 40u);
    CHECK_EQ(sz_size2index_tab.size(), 513u);
    CHECK_EQ(sz_pind2sz_tab.size(), expected.npsizes + 1);
    CHECK_EQ(default_sc_data.nlbins, 29);
    CHECK_EQ(default_sc_data.lg_ceil_nsizes, 8);
    CHECK_EQ(reinterpret_cast<uintptr_t>(sz_index2size_tab.data()) % 64, 0u);
    CHECK_EQ(reinterpret_cast<uintptr_t>(sz_size2index_tab.data()) % 64, 0u);
    CHECK_EQ(reinterpret_cast<uintptr_t>(sz_pind2sz_tab.data()) % 64, 0u);
}

TEST(SizeClasses, SmallClasses)
{
    REQUIRE(SC_NBINS <= std::size(small_reg_sizes));
    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        const SizeClass & sc = default_sc_data.sc[i];
        const BinInfo & info = bin_infos[i];
        CHECK_EQ(sc.index, int(i));
        CHECK(sc.bin);
        CHECK_EQ(sz_index2size_tab[i], small_reg_sizes[i]);
        CHECK_EQ(info.reg_size, small_reg_sizes[i]);
        CHECK_EQ(unsigned(sc.pgs), expected.pgs[i]);
        CHECK_EQ(info.slab_size, size_t(expected.pgs[i]) * PAGE);
        CHECK_EQ(info.nregs, expected.nregs[i]);
        CHECK_EQ(info.n_shards, 1u);
        CHECK_EQ(info.nregs * info.reg_size, info.slab_size);
        CHECK_EQ(sc.psz, small_reg_sizes[i] % PAGE == 0);
        CHECK_EQ(sc.lg_delta_lookup, small_reg_sizes[i] <= 4096 ? sc.lg_delta : 0);
        if (i < 36)
        {
            CHECK_EQ(sc.lg_base, small_lg[i][0]);
            CHECK_EQ(sc.lg_delta, small_lg[i][1]);
            CHECK_EQ(sc.ndelta, small_lg[i][2]);
            DivInfo div;
            div.init(info.reg_size);
            CHECK_EQ(div.magic, small_magic[i]);
        }
        DivInfo div;
        div.init(info.reg_size);
        CHECK_EQ(div.magic, uint32_t(((uint64_t(1) << 32) + info.reg_size - 1) / info.reg_size));
        for (size_t k = 0; k < info.nregs; ++k)
            CHECK_EQ(div.compute(k * info.reg_size), k);

        if constexpr (BITMAP_USE_TREE)
        {
            unsigned levels = i < expected.nbitmap ? expected.bitmap[i][0] : 1;
            unsigned groups = i < expected.nbitmap ? expected.bitmap[i][1] : 1;
            CHECK_EQ(info.bitmap_info.nbits, size_t(info.nregs));
            CHECK_EQ(bitmapLevels(info.bitmap_info), levels);
            CHECK_EQ(bitmapInfoNumGroups(info.bitmap_info), size_t(groups));
        }
        else
        {
            CHECK_EQ(info.bitmap_info.nbits, size_t(info.nregs));
            CHECK_EQ(bitmapInfoNumGroups(info.bitmap_info), size_t(groups_12[i]));
        }
    }
    /// The first large class.
    CHECK(!default_sc_data.sc[SC_NBINS].bin);
    CHECK_EQ(default_sc_data.sc[SC_NBINS].pgs, 0);
}

TEST(SizeClasses, LargeClasses)
{
    for (unsigned i = SC_NBINS; i < SC_NSIZES; ++i)
    {
        CHECK_EQ(sz_index2size_tab[i] % PAGE, 0u);
        CHECK(default_sc_data.sc[i].psz);
        CHECK(!default_sc_data.sc[i].bin);
    }
    CHECK_EQ(sz_index2size_tab[SC_NBINS], SC_LARGE_MINCLASS);
    CHECK_EQ(sz_index2size_tab[SC_NSIZES - 1], SC_LARGE_MAXCLASS);
    /// Groups of four: 2^b + k * 2^(b-2), k = 1..4 (the large classes start from the last class of the group with
    /// b = LG_PAGE + 1; the last group has three classes).
    for (unsigned b = LG_PAGE + 1; b <= 62; ++b)
    {
        for (unsigned k = (b == LG_PAGE + 1 ? 4 : 1); k <= (b == 62 ? 3 : 4); ++k)
        {
            unsigned index = SC_NBINS - 3 + (b - (LG_PAGE + 1)) * 4 + (k - 1);
            CHECK_EQ(sz_index2size_tab[index], (size_t(1) << b) + size_t(k) * (size_t(1) << (b - 2)));
        }
    }
    if constexpr (LG_PAGE == 12)
    {
        CHECK_EQ(sz_index2size_tab[36], 16384u);
        CHECK_EQ(sz_index2size_tab[37], 20480u);
        CHECK_EQ(sz_index2size_tab[40], 32768u);
        CHECK_EQ(sz_index2size_tab[44], 65536u);
        CHECK_EQ(sz_index2size_tab[60], 1048576u);
        CHECK_EQ(sz_index2size_tab[61], 1310720u);
    }
    CHECK_EQ(sz_index2size_tab[231], size_t(0x7000000000000000ULL));
}

TEST(SizeClasses, PageSizeClasses)
{
    CHECK_EQ(sz_pind2sz_tab[0], PAGE);
    CHECK_EQ(sz_pind2sz_tab[1], 2 * PAGE);
    CHECK_EQ(sz_pind2sz_tab[2], 3 * PAGE);
    CHECK_EQ(sz_pind2sz_tab[3], 4 * PAGE);
    for (unsigned p = 4; p < SC_NPSIZES; ++p)
    {
        unsigned g = p / 4;
        unsigned m = p % 4;
        CHECK_EQ(sz_pind2sz_tab[p], ((2 * PAGE) << g) + (m + 1) * (PAGE << (g - 1)));
    }
    CHECK_EQ(sz_pind2sz_tab[SC_NPSIZES - 1], SC_LARGE_MAXCLASS);
    CHECK_EQ(sz_pind2sz_tab[SC_NPSIZES], SC_LARGE_MAXCLASS + PAGE);
    for (unsigned p = 0; p <= SC_NPSIZES; ++p)
        CHECK_EQ(sz::pind2szCompute(p), sz_pind2sz_tab[p]);
    if constexpr (LG_PAGE == 12)
    {
        constexpr size_t first[] = {4096, 8192, 12288, 16384, 20480, 24576, 28672, 32768, 40960, 49152, 57344, 65536,
            81920, 98304, 114688, 131072};
        for (size_t i = 0; i < std::size(first); ++i)
            CHECK_EQ(sz_pind2sz_tab[i], first[i]);
        CHECK_EQ(sz_pind2sz_tab[198], size_t(0x7000000000000000ULL));
    }
    for (unsigned p = 0; p < SC_NPSIZES; ++p)
    {
        size_t psz = sz_pind2sz_tab[p];
        CHECK_EQ(sz::psz2ind(psz), p);
        CHECK_EQ(sz::psz2ind(psz + 1), p + 1);
        CHECK_EQ(sz::psz2u(psz), psz);
        CHECK_EQ(sz::psz2u(psz - 1), psz);
        CHECK_EQ(sz::psz2u(psz + 1), p + 1 < SC_NPSIZES ? sz_pind2sz_tab[p + 1] : SC_LARGE_MAXCLASS + PAGE);
    }
}

TEST(SizeClasses, SizeToIndexTable)
{
    constexpr uint8_t first[] = {0, 0, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9};
    for (size_t i = 0; i < std::size(first); ++i)
        CHECK_EQ(unsigned(sz_size2index_tab[i]), unsigned(first[i]));
    CHECK_EQ(unsigned(sz_size2index_tab[512]), 28u);
    for (size_t size = 0; size <= SC_LOOKUP_MAXCLASS; ++size)
    {
        szind_t ind = sz::sizeToIndex(size);
        CHECK_EQ(ind, sz::sizeToIndexCompute(size));
        size_t usize = sz_index2size_tab[ind];
        CHECK(usize >= size);
        if (ind > 0)
            CHECK(sz_index2size_tab[ind - 1] < size);
        CHECK_EQ(sz::s2u(size), usize);
        szind_t fast_ind;
        size_t fast_usize;
        sz::sizeToIndexUsizeFastpath(size, &fast_ind, &fast_usize);
        CHECK_EQ(fast_ind, ind);
        CHECK_EQ(fast_usize, usize);
    }
}

TEST(SizeClasses, IndexToSize)
{
    for (unsigned i = 0; i < SC_NSIZES; ++i)
    {
        CHECK_EQ(sz::indexToSizeCompute(i), sz_index2size_tab[i]);
        CHECK_EQ(sz::indexToSizeUnsafe(i), sz_index2size_tab[i]);
        CHECK_EQ(sz::sizeToIndex(sz_index2size_tab[i]), i);
        CHECK_EQ(sz::sizeToIndex(sz_index2size_tab[i] - 1), i);
        if (i + 1 < SC_NSIZES)
            CHECK_EQ(sz::sizeToIndex(sz_index2size_tab[i] + 1), i + 1);
    }
    CHECK_EQ(sz::sizeToIndex(SC_LARGE_MAXCLASS + 1), SC_NSIZES);
    CHECK_EQ(sz::sizeToIndex(~size_t(0)), SC_NSIZES);
    CHECK_EQ(sz::indexToSize(sz::sizeToIndex(USIZE_GROW_SLOW_THRESHOLD)), size_t(USIZE_GROW_SLOW_THRESHOLD));
}

TEST(SizeClasses, Usize)
{
    REQUIRE(sz::largeSizeClassesDisabled());
    CHECK_EQ(sz::s2u(0), 8u);
    CHECK_EQ(sz::s2u(1), 8u);
    CHECK_EQ(sz::s2u(9), 16u);
    CHECK_EQ(sz::s2u(SC_SMALL_MAXCLASS), SC_SMALL_MAXCLASS);
    CHECK_EQ(sz::s2u(SC_SMALL_MAXCLASS + 1), SC_LARGE_MINCLASS);
    CHECK_EQ(sz::s2u(SC_LARGE_MINCLASS + 1), SC_LARGE_MINCLASS + PAGE);
    CHECK_EQ(sz::s2u(SC_LARGE_MAXCLASS), SC_LARGE_MAXCLASS);
    CHECK_EQ(sz::s2u(SC_LARGE_MAXCLASS + 1), 0u);
    if constexpr (LG_PAGE == 12)
    {
        /// Spec 3.5.
        CHECK_EQ(sz::s2u(16385), 20480u);
        CHECK_EQ(sz::sizeToIndex(16385), 37u);
        CHECK_EQ(sz::s2u(32769), 36864u);
        CHECK_EQ(sz::sizeToIndex(32769), 41u);
        CHECK_EQ(sz::s2u(100000), 102400u);
        CHECK_EQ(sz::sizeToIndex(100000), 47u);
        CHECK_EQ(sz::s2u(1048577), 1052672u);
        CHECK_EQ(sz::sizeToIndex(1048577), 61u);
    }
    for (size_t size = SC_LOOKUP_MAXCLASS; size <= USIZE_GROW_SLOW_THRESHOLD; ++size)
        CHECK_EQ(sz::s2u(size), sz_index2size_tab[sz::sizeToIndex(size)]);

    opt.disable_large_size_classes = false;
    CHECK_EQ(sz::s2u(SC_LARGE_MINCLASS + 1), sz_index2size_tab[SC_NBINS + 1]);
    for (unsigned i = SC_NBINS; i < SC_NSIZES; ++i)
    {
        CHECK_EQ(sz::s2u(sz_index2size_tab[i]), sz_index2size_tab[i]);
        CHECK_EQ(sz::s2u(sz_index2size_tab[i] - 1), sz_index2size_tab[i]);
        CHECK_EQ(sz::indexToSize(i), sz_index2size_tab[i]);
    }
    opt.disable_large_size_classes = true;
}

TEST(SizeClasses, AlignedUsize)
{
    CHECK_EQ(sz_large_pad, PAGE);
    CHECK_EQ(sz::sa2u(1, 1), 8u);
    CHECK_EQ(sz::sa2u(1, 16), 16u);
    CHECK_EQ(sz::sa2u(17, 32), 32u);
    CHECK_EQ(sz::sa2u(96, 64), 128u);
    CHECK_EQ(sz::sa2u(1, PAGE), PAGE);
    CHECK_EQ(sz::sa2u(1, 2 * PAGE), SC_LARGE_MINCLASS);
    CHECK_EQ(sz::sa2u(SC_SMALL_MAXCLASS, 2), SC_SMALL_MAXCLASS);
    CHECK_EQ(sz::sa2u(SC_SMALL_MAXCLASS + 1, 2), SC_LARGE_MINCLASS);
    CHECK_EQ(sz::sa2u(SC_LARGE_MINCLASS + 1, 2), SC_LARGE_MINCLASS + PAGE);
    CHECK_EQ(sz::sa2u(1, size_t(1) << 63), 0u);
    CHECK_EQ(sz::sa2u(SC_LARGE_MAXCLASS + 1, 2), 0u);
    /// Not checked against SC_LARGE_MAXCLASS (callers do), and no overflow here.
    CHECK_EQ(sz::sa2u(SC_LARGE_MAXCLASS, size_t(1) << 62), SC_LARGE_MAXCLASS);
    CHECK_EQ(sz::sa2u(SC_LARGE_MAXCLASS - PAGE, size_t(1) << 62), SC_LARGE_MAXCLASS - PAGE);
    CHECK(sz::canUseSlab(SC_SMALL_MAXCLASS));
    CHECK(!sz::canUseSlab(SC_SMALL_MAXCLASS + 1));
}

TEST(SizeClasses, Quantize)
{
    REQUIRE(sz_large_pad == PAGE);
    if constexpr (LG_PAGE == 12)
    {
        /// Spec 4.5.
        CHECK_EQ(sz::pszQuantizeFloor(40960), 36864u);
        CHECK_EQ(sz::pszQuantizeCeil(40960), 45056u);
        CHECK_EQ(sz::pszQuantizeFloor(49152), 45056u);
        CHECK_EQ(sz::pszQuantizeCeil(49152), 53248u);
    }
    CHECK_EQ(sz::pszQuantizeFloor(PAGE), PAGE);
    CHECK_EQ(sz::pszQuantizeCeil(PAGE), PAGE);
    for (unsigned p = 0; p + 1 < SC_NPSIZES; ++p)
    {
        size_t size = sz_pind2sz_tab[p] + PAGE;
        CHECK_EQ(sz::pszQuantizeFloor(size), size);
        CHECK_EQ(sz::pszQuantizeCeil(size), size);
    }
    szBoot(default_sc_data, false);
    CHECK_EQ(sz_large_pad, 0u);
    CHECK_EQ(sz::pszQuantizeFloor(9 * PAGE), 8 * PAGE);
    CHECK_EQ(sz::pszQuantizeCeil(9 * PAGE), 10 * PAGE);
    szBoot(default_sc_data, true);
    CHECK_EQ(sz_large_pad, PAGE);
}

TEST(SizeClasses, SlabSizesOption)
{
    SizeClassData data;
    scBoot(data);
    CHECK(data.initialized);
    checkSameSizeClassData(data, default_sc_data);

    /// slab_sizes:1-4096:4 (the clamping gives at least ceil(reg / PAGE) and at most BITMAP_MAXBITS * reg / PAGE pages).
    scDataUpdateSlabSize(data, 1, 4096, 4);
    /// slab_sizes:0-1000000:1000000 clamps to the maximum for the classes above 4096.
    scDataUpdateSlabSize(data, 4097, 1000000, 1000000);
    unsigned shards[SC_NBINS];
    binShardSizesBoot(shards);
    for (unsigned s : shards)
        CHECK_EQ(s, 1u);
    CHECK(binUpdateShardSize(shards, 1, 160, 0));
    CHECK(binUpdateShardSize(shards, 1, 160, 65));
    CHECK(!binUpdateShardSize(shards, SC_SMALL_MAXCLASS + 1, ~size_t(0), 7));
    CHECK(!binUpdateShardSize(shards, 1, 160, 16));
    CHECK(!binUpdateShardSize(shards, 200, ~size_t(0), 64));
    binInfoBoot(data, shards);

    for (unsigned i = 0; i < SC_NBINS; ++i)
    {
        size_t reg = small_reg_sizes[i];
        size_t expected_pgs;
        if (reg <= 4096)
            expected_pgs = std::max<size_t>(4, (reg + PAGE - 1) / PAGE);
        else
            expected_pgs = BITMAP_MAXBITS * reg / PAGE;
        expected_pgs = std::min<size_t>(expected_pgs, BITMAP_MAXBITS * reg / PAGE);
        CHECK_EQ(size_t(data.sc[i].pgs), expected_pgs);
        CHECK_EQ(bin_infos[i].slab_size, expected_pgs * PAGE);
        CHECK_EQ(size_t(bin_infos[i].nregs), expected_pgs * PAGE / reg);
        CHECK(bin_infos[i].nregs <= BITMAP_MAXBITS);
        CHECK_EQ(bin_infos[i].bitmap_info.nbits, size_t(bin_infos[i].nregs));
        CHECK_EQ(bin_infos[i].n_shards, i <= 9 ? 16u : (i == 10 ? 1u : 64u));
    }

    /// A negative page count becomes huge (clamped to the maximum), as in jemalloc.
    scDataUpdateSlabSize(data, 8, 8, -1);
    CHECK_EQ(size_t(data.sc[0].pgs), BITMAP_MAXBITS * 8 / PAGE);
    scDataUpdateSlabSize(data, 8, 8, 0);
    CHECK_EQ(data.sc[0].pgs, 1);

    /// Back to the defaults.
    binShardSizesBoot(shards);
    binInfoBoot(default_sc_data, shards);
    for (unsigned i = 0; i < SC_NBINS; ++i)
        checkSameBinInfo(bin_infos[i], default_bin_infos[i]);
}
