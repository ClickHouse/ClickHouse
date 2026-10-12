/// Pins the size class tables to the values computed by jemalloc (`01-size-classes-bins.md` sections 1.1, 3, 4)
/// for the configured LOG2_PAGE (12, 14 or 16).

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <algorithm>

using namespace jemalloc;

namespace
{

constexpr size_t small_region_sizes[]
    = {8,     16,    32,    48,    64,    80,    96,    112,   128,   160,   192,   224,    256,    320,    384,    448,   512,   640,
       768,   896,   1024,  1280,  1536,  1792,  2048,  2560,  3072,  3584,  4096,  5120,   6144,   7168,   8192,   10240, 12288, 14336,
       16384, 20480, 24576, 28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, 163840, 196608, 229376};

/// lg_base / lg_delta / ndelta of classes 0..35 (identical for every page size).
constexpr int small_log2[36][3]
    = {{3, 3, 0},  {3, 3, 1},  {4, 4, 1},   {4, 4, 2},   {4, 4, 3},   {6, 4, 1},   {6, 4, 2},   {6, 4, 3},   {6, 4, 4},
       {7, 5, 1},  {7, 5, 2},  {7, 5, 3},   {7, 5, 4},   {8, 6, 1},   {8, 6, 2},   {8, 6, 3},   {8, 6, 4},   {9, 7, 1},
       {9, 7, 2},  {9, 7, 3},  {9, 7, 4},   {10, 8, 1},  {10, 8, 2},  {10, 8, 3},  {10, 8, 4},  {11, 9, 1},  {11, 9, 2},
       {11, 9, 3}, {11, 9, 4}, {12, 10, 1}, {12, 10, 2}, {12, 10, 3}, {12, 10, 4}, {13, 11, 1}, {13, 11, 2}, {13, 11, 3}};

/// div_info_t.magic of classes 0..35 (identical for every page size).
constexpr uint32_t small_magic[36]
    = {536870912, 268435456, 134217728, 89478486, 67108864, 53687092, 44739243, 38347923, 33554432, 26843546, 22369622, 19173962,
       16777216,  13421773,  11184811,  9586981,  8388608,  6710887,  5592406,  4793491,  4194304,  3355444,  2796203,  2396746,
       2097152,   1677722,   1398102,   1198373,  1048576,  838861,   699051,   599187,   524288,   419431,   349526,   299594};

constexpr unsigned pages_12[]
    = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 2, 5, 3, 7};
constexpr unsigned num_regions_12[] = {512, 256, 128, 256, 64, 256, 128, 256, 32, 128, 64, 128, 16, 64, 32, 64, 8, 32,
                                       16,  32,  4,   16,  8,  16,  2,   8,   4,  8,   1,  4,   2,  4,  1,  2,  1, 2};
constexpr unsigned groups_12[]
    = {8, 4, 2, 4, 1, 4, 2, 4, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

constexpr unsigned pages_14[]
    = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 2, 5, 3, 7};
constexpr unsigned num_regions_14[]
    = {2048, 1024, 512, 1024, 256, 1024, 512, 1024, 128, 512, 256, 512, 64, 256, 128, 256, 32, 128, 64, 128, 16, 64,
       32,   64,   8,   32,   16,  32,   4,   16,   8,   16,  2,   8,   4,  8,   1,   4,   2,  4,   1,  2,   1,  2};
/// levels:groups
constexpr unsigned bitmap_14[][2] = {{2, 33}, {2, 17}, {2, 9}, {2, 17}, {2, 5}, {2, 17}, {2, 9}, {2, 17}, {2, 3}, {2, 9},
                                     {2, 5},  {2, 9},  {1, 1}, {2, 5},  {2, 3}, {2, 5},  {1, 1}, {2, 3},  {1, 1}, {2, 3}};

constexpr unsigned pages_16[] = {1, 1, 1, 3, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5,
                                 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 1, 5, 3, 7, 2, 5, 3, 7};
constexpr unsigned num_regions_16[]
    = {8192, 4096, 2048, 4096, 1024, 4096, 2048, 4096, 512, 2048, 1024, 2048, 256, 1024, 512, 1024, 128, 512,
       256,  512,  64,   256,  128,  256,  32,   128,  64,  128,  16,   64,   32,  64,   8,   32,   16,  32,
       4,    16,   8,    16,   2,    8,    4,    8,    1,   4,    2,    4,    1,   2,    1,   2};
constexpr unsigned bitmap_16[][2]
    = {{3, 131}, {2, 65}, {2, 33}, {2, 65}, {2, 17}, {2, 65}, {2, 33}, {2, 65}, {2, 9}, {2, 33}, {2, 17}, {2, 33}, {2, 5}, {2, 17},
       {2, 9},   {2, 17}, {2, 3},  {2, 9},  {2, 5},  {2, 9},  {1, 1},  {2, 5},  {2, 3}, {2, 5},  {1, 1},  {2, 3},  {1, 1}, {2, 3}};

struct Expected
{
    unsigned num_bins;
    unsigned num_page_sizes;
    size_t small_max_class;
    size_t large_min_class;
    unsigned usable_size_grow_slow_threshold;
    unsigned log2_slab_max_regions;
    unsigned log2_bitmap_max_bits;
    bool use_tree;
    size_t groups_max;
    const unsigned * pages;
    const unsigned * num_regions;
    /// For the tree bitmap: levels:groups for the first `num_bitmap` bins, 1:1 for the rest. Null for the flat one.
    const unsigned (*bitmap)[2];
    size_t num_bitmap;
};

constexpr Expected expected = []
{
    if constexpr (LOG2_PAGE == 12)
        return Expected{36, 199, 14336, 16384, 32768, 9, 9, false, 8, pages_12, num_regions_12, nullptr, 0};
    else if constexpr (LOG2_PAGE == 14)
        return Expected{44, 191, 57344, 65536, 131072, 11, 11, true, 33, pages_14, num_regions_14, bitmap_14, std::size(bitmap_14)};
    else
        return Expected{52, 183, 229376, 262144, 524288, 13, 13, true, 131, pages_16, num_regions_16, bitmap_16, std::size(bitmap_16)};
}();

void checkSameSizeClassData(const SizeClassData & a, const SizeClassData & b)
{
    CHECK_EQ(a.num_tiny, b.num_tiny);
    CHECK_EQ(a.num_large_bins, b.num_large_bins);
    CHECK_EQ(a.num_bins, b.num_bins);
    CHECK_EQ(a.num_sizes, b.num_sizes);
    CHECK_EQ(a.log2_ceil_num_sizes, b.log2_ceil_num_sizes);
    CHECK_EQ(a.num_page_sizes, b.num_page_sizes);
    CHECK_EQ(a.log2_tiny_max_class, b.log2_tiny_max_class);
    CHECK_EQ(a.lookup_max_class, b.lookup_max_class);
    CHECK_EQ(a.small_max_class, b.small_max_class);
    CHECK_EQ(a.log2_large_min_class, b.log2_large_min_class);
    CHECK_EQ(a.large_min_class, b.large_min_class);
    CHECK_EQ(a.large_max_class, b.large_max_class);
    CHECK_EQ(a.initialized, b.initialized);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        CHECK_EQ(a.size_class[i].index, b.size_class[i].index);
        CHECK_EQ(a.size_class[i].log2_base, b.size_class[i].log2_base);
        CHECK_EQ(a.size_class[i].log2_delta, b.size_class[i].log2_delta);
        CHECK_EQ(a.size_class[i].num_delta, b.size_class[i].num_delta);
        CHECK_EQ(a.size_class[i].page_size, b.size_class[i].page_size);
        CHECK_EQ(a.size_class[i].bin, b.size_class[i].bin);
        CHECK_EQ(a.size_class[i].pages, b.size_class[i].pages);
        CHECK_EQ(a.size_class[i].log2_delta_lookup, b.size_class[i].log2_delta_lookup);
    }
}

template <bool UseTree>
void checkSameBitmapInfo(const BitmapInfoImpl<UseTree> & a, const BitmapInfoImpl<UseTree> & b)
{
    CHECK_EQ(a.num_bits, b.num_bits);
    if constexpr (UseTree)
    {
        CHECK_EQ(a.num_levels, b.num_levels);
        for (unsigned l = 0; l <= BITMAP_MAX_LEVELS; ++l)
            CHECK_EQ(a.levels[l].group_offset, b.levels[l].group_offset);
    }
    else
        CHECK_EQ(a.num_groups, b.num_groups);
}

template <bool UseTree>
unsigned bitmapLevels(const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
        return info.num_levels;
    else
        return 1;
}

void checkSameBinInfo(const BinInfo & a, const BinInfo & b)
{
    CHECK_EQ(a.region_size, b.region_size);
    CHECK_EQ(a.slab_size, b.slab_size);
    CHECK_EQ(a.num_regions, b.num_regions);
    CHECK_EQ(a.num_shards, b.num_shards);
    checkSameBitmapInfo(a.bitmap_info, b.bitmap_info);
}

}

TEST(SizeClasses, Constants)
{
    CHECK_EQ(SIZE_CLASS_NUM_SIZES, 232u);
    CHECK_EQ(SIZE_CLASS_NUM_TINY, 1u);
    CHECK_EQ(SIZE_CLASS_NUM_BINS, expected.num_bins);
    CHECK_EQ(SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS, 232u - expected.num_bins);
    CHECK_EQ(SIZE_CLASS_NUM_PAGE_SIZES, expected.num_page_sizes);
    CHECK_EQ(SIZE_CLASS_LOOKUP_MAX_CLASS, 4096u);
    CHECK_EQ(SIZE_CLASS_SMALL_MAX_CLASS, expected.small_max_class);
    CHECK_EQ(SIZE_CLASS_LARGE_MIN_CLASS, expected.large_min_class);
    CHECK_EQ(SIZE_CLASS_LARGE_MAX_CLASS, size_t(0x7000000000000000ULL));
    CHECK_EQ(SIZE_CLASS_LARGE_MAX_CLASS, size_t(8070450532247928832ULL));
    CHECK_EQ(USABLE_SIZE_GROW_SLOW_THRESHOLD, expected.usable_size_grow_slow_threshold);
    CHECK_EQ(SIZE_CLASS_LOG2_SLAB_MAX_REGIONS, expected.log2_slab_max_regions);
    CHECK_EQ(SIZE_CLASS_SLAB_MAX_REGIONS, 1u << expected.log2_slab_max_regions);
    CHECK_EQ(LOG2_BITMAP_MAX_BITS, expected.log2_bitmap_max_bits);
    CHECK_EQ(BITMAP_USE_TREE, expected.use_tree);
    CHECK_EQ(BITMAP_GROUPS_MAX, expected.groups_max);
    CHECK_EQ(sizeof(BitmapInfo), expected.use_tree ? 64u : 16u);
    CHECK_EQ(sizeof(BinInfo), expected.use_tree ? 88u : 40u);
    CHECK_EQ(size_to_index_table.size(), 513u);
    CHECK_EQ(page_size_class_idx_to_size_table.size(), expected.num_page_sizes + 1);
    CHECK_EQ(default_size_class_data.num_large_bins, 29);
    CHECK_EQ(default_size_class_data.log2_ceil_num_sizes, 8);
    CHECK_EQ(reinterpret_cast<uintptr_t>(index_to_size_table.data()) % 64, 0u);
    CHECK_EQ(reinterpret_cast<uintptr_t>(size_to_index_table.data()) % 64, 0u);
    CHECK_EQ(reinterpret_cast<uintptr_t>(page_size_class_idx_to_size_table.data()) % 64, 0u);
}

TEST(SizeClasses, SmallClasses)
{
    REQUIRE(SIZE_CLASS_NUM_BINS <= std::size(small_region_sizes));
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        const SizeClass & size_class = default_size_class_data.size_class[i];
        const BinInfo & info = bin_infos[i];
        CHECK_EQ(size_class.index, int(i));
        CHECK(size_class.bin);
        CHECK_EQ(index_to_size_table[i], small_region_sizes[i]);
        CHECK_EQ(info.region_size, small_region_sizes[i]);
        CHECK_EQ(unsigned(size_class.pages), expected.pages[i]);
        CHECK_EQ(info.slab_size, size_t(expected.pages[i]) * PAGE);
        CHECK_EQ(info.num_regions, expected.num_regions[i]);
        CHECK_EQ(info.num_shards, 1u);
        CHECK_EQ(info.num_regions * info.region_size, info.slab_size);
        CHECK_EQ(size_class.page_size, small_region_sizes[i] % PAGE == 0);
        CHECK_EQ(size_class.log2_delta_lookup, small_region_sizes[i] <= 4096 ? size_class.log2_delta : 0);
        if (i < 36)
        {
            CHECK_EQ(size_class.log2_base, small_log2[i][0]);
            CHECK_EQ(size_class.log2_delta, small_log2[i][1]);
            CHECK_EQ(size_class.num_delta, small_log2[i][2]);
            DivisionInfo division;
            division.init(info.region_size);
            CHECK_EQ(division.magic, small_magic[i]);
        }
        DivisionInfo division;
        division.init(info.region_size);
        CHECK_EQ(division.magic, uint32_t(((uint64_t(1) << 32) + info.region_size - 1) / info.region_size));
        for (size_t k = 0; k < info.num_regions; ++k)
            CHECK_EQ(division.compute(k * info.region_size), k);

        if constexpr (BITMAP_USE_TREE)
        {
            unsigned levels = i < expected.num_bitmap ? expected.bitmap[i][0] : 1;
            unsigned groups = i < expected.num_bitmap ? expected.bitmap[i][1] : 1;
            CHECK_EQ(info.bitmap_info.num_bits, size_t(info.num_regions));
            CHECK_EQ(bitmapLevels(info.bitmap_info), levels);
            CHECK_EQ(bitmapInfoNumGroups(info.bitmap_info), size_t(groups));
        }
        else
        {
            CHECK_EQ(info.bitmap_info.num_bits, size_t(info.num_regions));
            CHECK_EQ(bitmapInfoNumGroups(info.bitmap_info), size_t(groups_12[i]));
        }
    }
    /// The first large class.
    CHECK(!default_size_class_data.size_class[SIZE_CLASS_NUM_BINS].bin);
    CHECK_EQ(default_size_class_data.size_class[SIZE_CLASS_NUM_BINS].pages, 0);
}

TEST(SizeClasses, LargeClasses)
{
    for (unsigned i = SIZE_CLASS_NUM_BINS; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        CHECK_EQ(index_to_size_table[i] % PAGE, 0u);
        CHECK(default_size_class_data.size_class[i].page_size);
        CHECK(!default_size_class_data.size_class[i].bin);
    }
    CHECK_EQ(index_to_size_table[SIZE_CLASS_NUM_BINS], SIZE_CLASS_LARGE_MIN_CLASS);
    CHECK_EQ(index_to_size_table[SIZE_CLASS_NUM_SIZES - 1], SIZE_CLASS_LARGE_MAX_CLASS);
    /// Groups of four: 2^b + k * 2^(b-2), k = 1..4 (the large classes start from the last class of the group with
    /// b = LOG2_PAGE + 1; the last group has three classes).
    for (unsigned b = LOG2_PAGE + 1; b <= 62; ++b)
    {
        for (unsigned k = (b == LOG2_PAGE + 1 ? 4 : 1); k <= (b == 62 ? 3 : 4); ++k)
        {
            unsigned index = SIZE_CLASS_NUM_BINS - 3 + (b - (LOG2_PAGE + 1)) * 4 + (k - 1);
            CHECK_EQ(index_to_size_table[index], (size_t(1) << b) + size_t(k) * (size_t(1) << (b - 2)));
        }
    }
    if constexpr (LOG2_PAGE == 12)
    {
        CHECK_EQ(index_to_size_table[36], 16384u);
        CHECK_EQ(index_to_size_table[37], 20480u);
        CHECK_EQ(index_to_size_table[40], 32768u);
        CHECK_EQ(index_to_size_table[44], 65536u);
        CHECK_EQ(index_to_size_table[60], 1048576u);
        CHECK_EQ(index_to_size_table[61], 1310720u);
    }
    CHECK_EQ(index_to_size_table[231], size_t(0x7000000000000000ULL));
}

TEST(SizeClasses, PageSizeClasses)
{
    CHECK_EQ(page_size_class_idx_to_size_table[0], PAGE);
    CHECK_EQ(page_size_class_idx_to_size_table[1], 2 * PAGE);
    CHECK_EQ(page_size_class_idx_to_size_table[2], 3 * PAGE);
    CHECK_EQ(page_size_class_idx_to_size_table[3], 4 * PAGE);
    for (unsigned p = 4; p < SIZE_CLASS_NUM_PAGE_SIZES; ++p)
    {
        unsigned g = p / 4;
        unsigned m = p % 4;
        CHECK_EQ(page_size_class_idx_to_size_table[p], ((2 * PAGE) << g) + (m + 1) * (PAGE << (g - 1)));
    }
    CHECK_EQ(page_size_class_idx_to_size_table[SIZE_CLASS_NUM_PAGE_SIZES - 1], SIZE_CLASS_LARGE_MAX_CLASS);
    CHECK_EQ(page_size_class_idx_to_size_table[SIZE_CLASS_NUM_PAGE_SIZES], SIZE_CLASS_LARGE_MAX_CLASS + PAGE);
    for (unsigned p = 0; p <= SIZE_CLASS_NUM_PAGE_SIZES; ++p)
        CHECK_EQ(size_classes::pageSizeClassIdxToSizeCompute(p), page_size_class_idx_to_size_table[p]);
    if constexpr (LOG2_PAGE == 12)
    {
        constexpr size_t first[]
            = {4096, 8192, 12288, 16384, 20480, 24576, 28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072};
        for (size_t i = 0; i < std::size(first); ++i)
            CHECK_EQ(page_size_class_idx_to_size_table[i], first[i]);
        CHECK_EQ(page_size_class_idx_to_size_table[198], size_t(0x7000000000000000ULL));
    }
    for (unsigned p = 0; p < SIZE_CLASS_NUM_PAGE_SIZES; ++p)
    {
        size_t page_size = page_size_class_idx_to_size_table[p];
        CHECK_EQ(size_classes::pageSizeToPageSizeClassIdx(page_size), p);
        CHECK_EQ(size_classes::pageSizeToPageSizeClassIdx(page_size + 1), p + 1);
        CHECK_EQ(size_classes::pageSizeToUsableSize(page_size), page_size);
        CHECK_EQ(size_classes::pageSizeToUsableSize(page_size - 1), page_size);
        CHECK_EQ(
            size_classes::pageSizeToUsableSize(page_size + 1),
            p + 1 < SIZE_CLASS_NUM_PAGE_SIZES ? page_size_class_idx_to_size_table[p + 1] : SIZE_CLASS_LARGE_MAX_CLASS + PAGE);
    }
}

TEST(SizeClasses, SizeToIndexTable)
{
    constexpr uint8_t first[] = {0, 0, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9};
    for (size_t i = 0; i < std::size(first); ++i)
        CHECK_EQ(unsigned(size_to_index_table[i]), unsigned(first[i]));
    CHECK_EQ(unsigned(size_to_index_table[512]), 28u);
    for (size_t size = 0; size <= SIZE_CLASS_LOOKUP_MAX_CLASS; ++size)
    {
        SizeClassIdx idx = size_classes::sizeToIndex(size);
        CHECK_EQ(idx, size_classes::sizeToIndexCompute(size));
        size_t usable_size = index_to_size_table[idx];
        CHECK(usable_size >= size);
        if (idx > 0)
            CHECK(index_to_size_table[idx - 1] < size);
        CHECK_EQ(size_classes::sizeToUsableSize(size), usable_size);
        SizeClassIdx fast_idx;
        size_t fast_usable_size;
        size_classes::sizeToIndexUsableSizeFastPath(size, &fast_idx, &fast_usable_size);
        CHECK_EQ(fast_idx, idx);
        CHECK_EQ(fast_usable_size, usable_size);
    }
}

TEST(SizeClasses, IndexToSize)
{
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        CHECK_EQ(size_classes::indexToSizeCompute(i), index_to_size_table[i]);
        CHECK_EQ(size_classes::indexToSizeUnsafe(i), index_to_size_table[i]);
        CHECK_EQ(size_classes::sizeToIndex(index_to_size_table[i]), i);
        CHECK_EQ(size_classes::sizeToIndex(index_to_size_table[i] - 1), i);
        if (i + 1 < SIZE_CLASS_NUM_SIZES)
            CHECK_EQ(size_classes::sizeToIndex(index_to_size_table[i] + 1), i + 1);
    }
    CHECK_EQ(size_classes::sizeToIndex(SIZE_CLASS_LARGE_MAX_CLASS + 1), SIZE_CLASS_NUM_SIZES);
    CHECK_EQ(size_classes::sizeToIndex(~size_t(0)), SIZE_CLASS_NUM_SIZES);
    CHECK_EQ(
        size_classes::indexToSize(size_classes::sizeToIndex(USABLE_SIZE_GROW_SLOW_THRESHOLD)), size_t(USABLE_SIZE_GROW_SLOW_THRESHOLD));
}

TEST(SizeClasses, UsableSize)
{
    REQUIRE(size_classes::largeSizeClassesDisabled());
    CHECK_EQ(size_classes::sizeToUsableSize(0), 8u);
    CHECK_EQ(size_classes::sizeToUsableSize(1), 8u);
    CHECK_EQ(size_classes::sizeToUsableSize(9), 16u);
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_SMALL_MAX_CLASS), SIZE_CLASS_SMALL_MAX_CLASS);
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_SMALL_MAX_CLASS + 1), SIZE_CLASS_LARGE_MIN_CLASS);
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_LARGE_MIN_CLASS + 1), SIZE_CLASS_LARGE_MIN_CLASS + PAGE);
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_LARGE_MAX_CLASS), SIZE_CLASS_LARGE_MAX_CLASS);
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_LARGE_MAX_CLASS + 1), 0u);
    if constexpr (LOG2_PAGE == 12)
    {
        /// Spec 3.5.
        CHECK_EQ(size_classes::sizeToUsableSize(16385), 20480u);
        CHECK_EQ(size_classes::sizeToIndex(16385), 37u);
        CHECK_EQ(size_classes::sizeToUsableSize(32769), 36864u);
        CHECK_EQ(size_classes::sizeToIndex(32769), 41u);
        CHECK_EQ(size_classes::sizeToUsableSize(100000), 102400u);
        CHECK_EQ(size_classes::sizeToIndex(100000), 47u);
        CHECK_EQ(size_classes::sizeToUsableSize(1048577), 1052672u);
        CHECK_EQ(size_classes::sizeToIndex(1048577), 61u);
    }
    for (size_t size = SIZE_CLASS_LOOKUP_MAX_CLASS; size <= USABLE_SIZE_GROW_SLOW_THRESHOLD; ++size)
        CHECK_EQ(size_classes::sizeToUsableSize(size), index_to_size_table[size_classes::sizeToIndex(size)]);

    options.disable_large_size_classes = false;
    CHECK_EQ(size_classes::sizeToUsableSize(SIZE_CLASS_LARGE_MIN_CLASS + 1), index_to_size_table[SIZE_CLASS_NUM_BINS + 1]);
    for (unsigned i = SIZE_CLASS_NUM_BINS; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        CHECK_EQ(size_classes::sizeToUsableSize(index_to_size_table[i]), index_to_size_table[i]);
        CHECK_EQ(size_classes::sizeToUsableSize(index_to_size_table[i] - 1), index_to_size_table[i]);
        CHECK_EQ(size_classes::indexToSize(i), index_to_size_table[i]);
    }
    options.disable_large_size_classes = true;
}

TEST(SizeClasses, AlignedUsableSize)
{
    CHECK_EQ(large_pad, PAGE);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(1, 1), 8u);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(1, 16), 16u);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(17, 32), 32u);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(96, 64), 128u);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(1, PAGE), PAGE);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(1, 2 * PAGE), SIZE_CLASS_LARGE_MIN_CLASS);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_SMALL_MAX_CLASS, 2), SIZE_CLASS_SMALL_MAX_CLASS);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_SMALL_MAX_CLASS + 1, 2), SIZE_CLASS_LARGE_MIN_CLASS);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_LARGE_MIN_CLASS + 1, 2), SIZE_CLASS_LARGE_MIN_CLASS + PAGE);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(1, size_t(1) << 63), 0u);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_LARGE_MAX_CLASS + 1, 2), 0u);
    /// Not checked against SIZE_CLASS_LARGE_MAX_CLASS (callers do), and no overflow here.
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_LARGE_MAX_CLASS, size_t(1) << 62), SIZE_CLASS_LARGE_MAX_CLASS);
    CHECK_EQ(size_classes::alignedSizeToUsableSize(SIZE_CLASS_LARGE_MAX_CLASS - PAGE, size_t(1) << 62), SIZE_CLASS_LARGE_MAX_CLASS - PAGE);
    CHECK(size_classes::canUseSlab(SIZE_CLASS_SMALL_MAX_CLASS));
    CHECK(!size_classes::canUseSlab(SIZE_CLASS_SMALL_MAX_CLASS + 1));
}

TEST(SizeClasses, Quantize)
{
    REQUIRE(large_pad == PAGE);
    if constexpr (LOG2_PAGE == 12)
    {
        /// Spec 4.5.
        CHECK_EQ(size_classes::pageSizeQuantizeFloor(40960), 36864u);
        CHECK_EQ(size_classes::pageSizeQuantizeCeil(40960), 45056u);
        CHECK_EQ(size_classes::pageSizeQuantizeFloor(49152), 45056u);
        CHECK_EQ(size_classes::pageSizeQuantizeCeil(49152), 53248u);
    }
    CHECK_EQ(size_classes::pageSizeQuantizeFloor(PAGE), PAGE);
    CHECK_EQ(size_classes::pageSizeQuantizeCeil(PAGE), PAGE);
    for (unsigned p = 0; p + 1 < SIZE_CLASS_NUM_PAGE_SIZES; ++p)
    {
        size_t size = page_size_class_idx_to_size_table[p] + PAGE;
        CHECK_EQ(size_classes::pageSizeQuantizeFloor(size), size);
        CHECK_EQ(size_classes::pageSizeQuantizeCeil(size), size);
    }
    sizeBoot(default_size_class_data, false);
    CHECK_EQ(large_pad, 0u);
    CHECK_EQ(size_classes::pageSizeQuantizeFloor(9 * PAGE), 8 * PAGE);
    CHECK_EQ(size_classes::pageSizeQuantizeCeil(9 * PAGE), 10 * PAGE);
    sizeBoot(default_size_class_data, true);
    CHECK_EQ(large_pad, PAGE);
}

TEST(SizeClasses, SlabSizesOption)
{
    SizeClassData data;
    sizeClassBoot(data);
    CHECK(data.initialized);
    checkSameSizeClassData(data, default_size_class_data);

    /// slab_sizes:1-4096:4 (the clamping gives at least ceil(reg / PAGE) and at most BITMAP_MAX_BITS * reg / PAGE pages).
    sizeClassDataUpdateSlabSize(data, 1, 4096, 4);
    /// slab_sizes:0-1000000:1000000 clamps to the maximum for the classes above 4096.
    sizeClassDataUpdateSlabSize(data, 4097, 1000000, 1000000);
    unsigned shards[SIZE_CLASS_NUM_BINS];
    binShardSizesBoot(shards);
    for (unsigned s : shards)
        CHECK_EQ(s, 1u);
    CHECK(binUpdateShardSize(shards, 1, 160, 0));
    CHECK(binUpdateShardSize(shards, 1, 160, 65));
    CHECK(!binUpdateShardSize(shards, SIZE_CLASS_SMALL_MAX_CLASS + 1, ~size_t(0), 7));
    CHECK(!binUpdateShardSize(shards, 1, 160, 16));
    CHECK(!binUpdateShardSize(shards, 200, ~size_t(0), 64));
    binInfoBoot(data, shards);

    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        size_t region = small_region_sizes[i];
        size_t expected_pages;
        if (region <= 4096)
            expected_pages = std::max<size_t>(4, (region + PAGE - 1) / PAGE);
        else
            expected_pages = BITMAP_MAX_BITS * region / PAGE;
        expected_pages = std::min<size_t>(expected_pages, BITMAP_MAX_BITS * region / PAGE);
        CHECK_EQ(size_t(data.size_class[i].pages), expected_pages);
        CHECK_EQ(bin_infos[i].slab_size, expected_pages * PAGE);
        CHECK_EQ(size_t(bin_infos[i].num_regions), expected_pages * PAGE / region);
        CHECK(bin_infos[i].num_regions <= BITMAP_MAX_BITS);
        CHECK_EQ(bin_infos[i].bitmap_info.num_bits, size_t(bin_infos[i].num_regions));
        CHECK_EQ(bin_infos[i].num_shards, i <= 9 ? 16u : (i == 10 ? 1u : 64u));
    }

    /// A negative page count becomes huge (clamped to the maximum), as in jemalloc.
    sizeClassDataUpdateSlabSize(data, 8, 8, -1);
    CHECK_EQ(size_t(data.size_class[0].pages), BITMAP_MAX_BITS * 8 / PAGE);
    sizeClassDataUpdateSlabSize(data, 8, 8, 0);
    CHECK_EQ(data.size_class[0].pages, 1);

    /// Back to the defaults.
    binShardSizesBoot(shards);
    binInfoBoot(default_size_class_data, shards);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        checkSameBinInfo(bin_infos[i], default_bin_infos[i]);
}
