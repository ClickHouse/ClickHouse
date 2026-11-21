/// Compares the size class tables and every size computation with jemalloc's C implementation
/// (`size_classes_oracle_ref.c`, linked with the reference `lib_jemalloc.a`), for a dense set of sizes, all
/// alignments, every combination of `disable_large_size_classes` and `cache_oblivious`, and randomized
/// `slab_sizes` / `bin_shards` updates.

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <vector>

using namespace jemalloc;

extern "C" {
size_t ref_constant(int which);
void ref_boot(bool cache_oblivious);
void ref_size_classes_boot(bool cache_oblivious);
void ref_size_class_data_init();
void ref_size_class_update_slab_size(size_t begin, size_t end, int pages);
void ref_bin_shard_sizes_boot();
bool ref_bin_update_shard_size(size_t start, size_t end, size_t num_shards);
unsigned ref_bin_shard(unsigned i);
void ref_bin_info_boot();
void ref_size_class_summary(size_t * out);
void ref_size_class(unsigned i, int * out);
void ref_bin_info(unsigned i, size_t * out);
void ref_set_disable_large_size_classes(bool value);
size_t ref_large_pad();
size_t ref_index_to_size_table(unsigned i);
unsigned ref_size_to_index_table(unsigned i);
size_t ref_page_size_class_idx_to_size_table(unsigned i);
unsigned ref_size_to_index(size_t size);
unsigned ref_size_to_index_compute(size_t size);
unsigned ref_size_to_index_lookup(size_t size);
void ref_size_to_index_usable_size_fast_path(size_t size, unsigned * idx, size_t * usable_size);
size_t ref_index_to_size_compute(unsigned index);
size_t ref_index_to_size_unsafe(unsigned index);
size_t ref_index_to_size(unsigned index);
size_t ref_s2u(size_t size);
size_t ref_s2u_compute(size_t size);
size_t ref_aligned_size_to_usable_size(size_t size, size_t alignment);
unsigned ref_page_size_to_idx(size_t page_size);
size_t ref_page_size_class_idx_to_size(unsigned page_size_class_idx);
size_t ref_page_size_class_idx_to_size_compute(unsigned page_size_class_idx);
size_t ref_page_size_to_usable_size(size_t page_size);
size_t ref_page_size_quantize_floor(size_t size);
size_t ref_page_size_quantize_ceil(size_t size);
bool ref_can_use_slab(size_t size);
bool ref_large_size_classes_disabled();
uint32_t ref_division_magic(size_t d);
size_t ref_division_compute(size_t d, size_t n);
}

namespace
{

/// The state of the C++ side that mirrors the reference's static `size_class_data` and shard sizes.
SizeClassData size_class_data;
unsigned shards[SIZE_CLASS_NUM_BINS];

void bootBoth(bool cache_oblivious)
{
    ref_boot(cache_oblivious);
    sizeClassBoot(size_class_data);
    binShardSizesBoot(shards);
    sizeBoot(size_class_data, cache_oblivious);
    binInfoBoot(size_class_data, shards);
}

void setDisableLargeSizeClasses(bool value)
{
    ref_set_disable_large_size_classes(value);
    options.disable_large_size_classes = value;
}

template <bool UseTree>
void bitmapInfoFields(const BitmapInfoImpl<UseTree> & info, size_t * out)
{
    out[0] = info.num_bits;
    if constexpr (UseTree)
    {
        out[1] = info.num_levels;
        for (unsigned l = 0; l <= BITMAP_MAX_LEVELS; ++l)
            out[2 + l] = info.levels[l].group_offset;
    }
    else
        out[1] = info.num_groups;
}

void compareSizeClassData()
{
    size_t ref_summary[13];
    ref_size_class_summary(ref_summary);
    CHECK_EQ(ref_summary[0], size_t(size_class_data.num_tiny));
    CHECK_EQ(ref_summary[1], size_t(size_class_data.num_large_bins));
    CHECK_EQ(ref_summary[2], size_t(size_class_data.num_bins));
    CHECK_EQ(ref_summary[3], size_t(size_class_data.num_sizes));
    CHECK_EQ(ref_summary[4], size_t(size_class_data.log2_ceil_num_sizes));
    CHECK_EQ(ref_summary[5], size_t(size_class_data.num_page_sizes));
    CHECK_EQ(ref_summary[6], size_t(size_class_data.log2_tiny_max_class));
    CHECK_EQ(ref_summary[7], size_class_data.lookup_max_class);
    CHECK_EQ(ref_summary[8], size_class_data.small_max_class);
    CHECK_EQ(ref_summary[9], size_t(size_class_data.log2_large_min_class));
    CHECK_EQ(ref_summary[10], size_class_data.large_min_class);
    CHECK_EQ(ref_summary[11], size_class_data.large_max_class);
    CHECK_EQ(ref_summary[12], size_t(size_class_data.initialized));

    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        int ref[8];
        ref_size_class(i, ref);
        const SizeClass & size_class = size_class_data.size_class[i];
        CHECK_EQ(ref[0], size_class.index);
        CHECK_EQ(ref[1], size_class.log2_base);
        CHECK_EQ(ref[2], size_class.log2_delta);
        CHECK_EQ(ref[3], size_class.num_delta);
        CHECK_EQ(ref[4], int(size_class.page_size));
        CHECK_EQ(ref[5], int(size_class.bin));
        CHECK_EQ(ref[6], size_class.pages);
        CHECK_EQ(ref[7], size_class.log2_delta_lookup);
    }
}

void compareBinInfos()
{
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        size_t ref[12] = {};
        size_t own[12] = {};
        ref_bin_info(i, ref);
        const BinInfo & info = bin_infos[i];
        own[0] = info.region_size;
        own[1] = info.slab_size;
        own[2] = info.num_regions;
        own[3] = info.num_shards;
        bitmapInfoFields(info.bitmap_info, own + 4);
        for (unsigned k = 0; k < 12; ++k)
            CHECK_EQ(own[k], ref[k]);
        CHECK_EQ(shards[i], ref_bin_shard(i));
    }
}

void compareTables()
{
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
        CHECK_EQ(index_to_size_table[i], ref_index_to_size_table(i));
    for (unsigned i = 0; i < size_to_index_table.size(); ++i)
        CHECK_EQ(unsigned(size_to_index_table[i]), ref_size_to_index_table(i));
    for (unsigned i = 0; i <= SIZE_CLASS_NUM_PAGE_SIZES; ++i)
        CHECK_EQ(page_size_class_idx_to_size_table[i], ref_page_size_class_idx_to_size_table(i));
}

std::vector<size_t> makeSizes()
{
    std::vector<size_t> sizes;
    for (size_t size = 0; size <= 70000; ++size)
        sizes.push_back(size);
    for (unsigned log2_size = 0; log2_size < 64; ++log2_size)
    {
        size_t base = size_t(1) << log2_size;
        /// The power of two and the other class boundaries of its group, with small deltas.
        for (size_t quarter = 0; quarter < 4; ++quarter)
        {
            size_t point = base + quarter * (base >> 2);
            for (int delta = -20; delta <= 20; ++delta)
                sizes.push_back(point + size_t(ptrdiff_t(delta)));
            for (int pages = -3; pages <= 3; ++pages)
                sizes.push_back(point + size_t(ptrdiff_t(pages)) * PAGE);
        }
    }
    for (int delta = -100; delta <= 100; ++delta)
    {
        sizes.push_back(SIZE_CLASS_LARGE_MAX_CLASS + size_t(ptrdiff_t(delta)));
        sizes.push_back(SIZE_CLASS_LARGE_MAX_CLASS + PAGE + size_t(ptrdiff_t(delta)));
        sizes.push_back(size_t(ptrdiff_t(delta)));
    }
    for (int pages = -10; pages <= 10; ++pages)
        sizes.push_back(SIZE_CLASS_LARGE_MAX_CLASS + size_t(ptrdiff_t(pages)) * PAGE);
    /// A pseudo-random sample of all magnitudes.
    uint64_t x = 0x9E3779B97F4A7C15ULL;
    for (int i = 0; i < 200000; ++i)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        sizes.push_back(size_t(x >> (x % 64)));
    }
    return sizes;
}

void compareLookups(const std::vector<size_t> & sizes, bool with_alignments)
{
    CHECK_EQ(large_pad, ref_large_pad());
    CHECK_EQ(size_classes::largeSizeClassesDisabled(), ref_large_size_classes_disabled());

    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        CHECK_EQ(size_classes::indexToSizeCompute(i), ref_index_to_size_compute(i));
        CHECK_EQ(size_classes::indexToSizeUnsafe(i), ref_index_to_size_unsafe(i));
        if (!size_classes::largeSizeClassesDisabled() || i <= size_classes::sizeToIndex(USABLE_SIZE_GROW_SLOW_THRESHOLD))
            CHECK_EQ(size_classes::indexToSize(i), ref_index_to_size(i));
    }
    for (unsigned p = 0; p <= SIZE_CLASS_NUM_PAGE_SIZES; ++p)
    {
        CHECK_EQ(size_classes::pageSizeClassIdxToSizeCompute(p), ref_page_size_class_idx_to_size_compute(p));
        CHECK_EQ(size_classes::pageSizeClassIdxToSize(p), ref_page_size_class_idx_to_size(p));
    }

    size_t failures_before = size_t(allocator_test::failureCount());
    for (size_t size : sizes)
    {
        CHECK_EQ(size_classes::sizeToIndex(size), ref_size_to_index(size));
        CHECK_EQ(size_classes::sizeToIndexCompute(size), ref_size_to_index_compute(size));
        CHECK_EQ(size_classes::sizeToUsableSize(size), ref_s2u(size));
        CHECK_EQ(size_classes::sizeToUsableSizeCompute(size), ref_s2u_compute(size));
        CHECK_EQ(size_classes::canUseSlab(size), ref_can_use_slab(size));
        if (size <= SIZE_CLASS_LOOKUP_MAX_CLASS)
        {
            CHECK_EQ(size_classes::sizeToIndexLookup(size), ref_size_to_index_lookup(size));
            SizeClassIdx idx;
            size_t usable_size;
            unsigned ref_idx;
            size_t ref_usable_size;
            size_classes::sizeToIndexUsableSizeFastPath(size, &idx, &usable_size);
            ref_size_to_index_usable_size_fast_path(size, &ref_idx, &ref_usable_size);
            CHECK_EQ(idx, ref_idx);
            CHECK_EQ(usable_size, ref_usable_size);
        }
        if (size > 0)
        {
            CHECK_EQ(size_classes::pageSizeToPageSizeClassIdx(size), ref_page_size_to_idx(size));
            CHECK_EQ(size_classes::pageSizeToUsableSize(size), ref_page_size_to_usable_size(size));
        }
        if (size > 0 && (size & PAGE_MASK) == 0 && size >= large_pad && size - large_pad <= SIZE_CLASS_LARGE_MAX_CLASS)
        {
            CHECK_EQ(size_classes::pageSizeQuantizeFloor(size), ref_page_size_quantize_floor(size));
            CHECK_EQ(size_classes::pageSizeQuantizeCeil(size), ref_page_size_quantize_ceil(size));
        }
        if (with_alignments)
        {
            for (unsigned log2_align = 0; log2_align < 64; ++log2_align)
                CHECK_EQ(
                    size_classes::alignedSizeToUsableSize(size, size_t(1) << log2_align),
                    ref_aligned_size_to_usable_size(size, size_t(1) << log2_align));
        }
        /// Do not flood the output.
        REQUIRE(size_t(allocator_test::failureCount()) < failures_before + 100);
    }
}

}

TEST(SizeClassesOracle, Constants)
{
    REQUIRE(ref_constant(3) == PAGE);
    CHECK_EQ(ref_constant(0), size_t(SIZE_CLASS_NUM_SIZES));
    CHECK_EQ(ref_constant(1), size_t(SIZE_CLASS_NUM_BINS));
    CHECK_EQ(ref_constant(2), size_t(SIZE_CLASS_NUM_PAGE_SIZES));
    CHECK_EQ(ref_constant(4), SIZE_CLASS_SMALL_MAX_CLASS);
    CHECK_EQ(ref_constant(5), SIZE_CLASS_LARGE_MIN_CLASS);
    CHECK_EQ(ref_constant(6), SIZE_CLASS_LARGE_MAX_CLASS);
    CHECK_EQ(ref_constant(7), SIZE_CLASS_LOOKUP_MAX_CLASS);
    CHECK_EQ(ref_constant(8), size_t(SIZE_CLASS_LOG2_SLAB_MAX_REGIONS));
    CHECK_EQ(ref_constant(9), size_t(LOG2_BITMAP_MAX_BITS));
    CHECK_EQ(ref_constant(10), BITMAP_MAX_BITS);
    CHECK_EQ(ref_constant(11), BITMAP_GROUPS_MAX);
    CHECK_EQ(ref_constant(12), sizeof(BitmapInfo));
    CHECK_EQ(ref_constant(13), sizeof(BinInfo));
    CHECK_EQ(ref_constant(14), size_t(USABLE_SIZE_GROW_SLOW_THRESHOLD));
    CHECK_EQ(ref_constant(15), size_t(SIZE_CLASS_NUM_TINY));
    CHECK_EQ(ref_constant(16), size_to_index_table.size());
    CHECK_EQ(ref_constant(17), size_t(BIN_SHARDS_MAX));
}

TEST(SizeClassesOracle, Tables)
{
    bootBoth(true);
    compareSizeClassData();
    compareBinInfos();
    compareTables();

    /// The compile-time defaults are the same as the boot result.
    size_class_data = default_size_class_data;
    compareSizeClassData();
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        size_t ref[12] = {};
        size_t own[12] = {};
        ref_bin_info(i, ref);
        const BinInfo & info = default_bin_infos[i];
        own[0] = info.region_size;
        own[1] = info.slab_size;
        own[2] = info.num_regions;
        own[3] = info.num_shards;
        bitmapInfoFields(info.bitmap_info, own + 4);
        for (unsigned k = 0; k < 12; ++k)
            CHECK_EQ(own[k], ref[k]);
    }
}

TEST(SizeClassesOracle, Lookups)
{
    std::vector<size_t> sizes = makeSizes();
    for (bool cache_oblivious : {true, false})
    {
        for (bool disable_large : {true, false})
        {
            bootBoth(cache_oblivious);
            setDisableLargeSizeClasses(disable_large);
            compareLookups(sizes, /* with_alignments */ false);
        }
    }
    setDisableLargeSizeClasses(true);
    bootBoth(true);
}

TEST(SizeClassesOracle, AlignedLookups)
{
    /// sa2u with every power of two alignment, on a smaller set of sizes.
    std::vector<size_t> all = makeSizes();
    std::vector<size_t> sizes;
    for (size_t i = 0; i < all.size(); ++i)
        if (all[i] > 70000 || all[i] % 7 == 0 || all[i] < 300)
            sizes.push_back(all[i]);
    for (bool cache_oblivious : {true, false})
    {
        for (bool disable_large : {true, false})
        {
            bootBoth(cache_oblivious);
            setDisableLargeSizeClasses(disable_large);
            compareLookups(sizes, /* with_alignments */ true);
        }
    }
    setDisableLargeSizeClasses(true);
    bootBoth(true);
}

TEST(SizeClassesOracle, Division)
{
    for (size_t d = 2; d <= 600000; ++d)
    {
        DivisionInfo division;
        division.init(d);
        CHECK_EQ(division.magic, ref_division_magic(d));
        if (d % 997 == 0)
        {
            for (size_t k = 0; k * d < (size_t(1) << 32) && k < 100000; k += 1 + k / 8)
                CHECK_EQ(division.compute(k * d), ref_division_compute(d, k * d));
        }
    }
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        DivisionInfo division;
        division.init(bin_infos[i].region_size);
        for (size_t k = 0; k < bin_infos[i].num_regions; ++k)
            CHECK_EQ(
                division.compute(k * bin_infos[i].region_size),
                ref_division_compute(bin_infos[i].region_size, k * bin_infos[i].region_size));
    }
}

TEST(SizeClassesOracle, SlabSizesAndShards)
{
    uint64_t x = 0x2545F4914F6CDD1DULL;
    auto next = [&x]
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    };
    auto randomSize = [&]
    {
        uint64_t r = next();
        switch (r % 4)
        {
            case 0: return size_t(r >> 40) % (SIZE_CLASS_SMALL_MAX_CLASS + 100);
            case 1: return bin_infos[(r >> 8) % SIZE_CLASS_NUM_BINS].region_size + size_t((r >> 16) % 3) - 1;
            case 2: return size_t(r >> (r % 64));
            default: return size_t(0);
        }
    };

    for (int round = 0; round < 300; ++round)
    {
        bootBoth(true);
        int num_updates = int(next() % 6);
        for (int u = 0; u < num_updates; ++u)
        {
            size_t begin = randomSize();
            size_t end = (next() % 4 == 0) ? ~size_t(0) : randomSize();
            uint64_t r = next();
            int pages = (r % 5 == 0) ? int(int64_t(r >> 32)) : int((r >> 8) % 300) - 2;
            if (next() % 10 == 0)
            {
                /// `slab_sizes:default`.
                ref_size_class_data_init();
                sizeClassDataInit(size_class_data);
            }
            ref_size_class_update_slab_size(begin, end, pages);
            sizeClassDataUpdateSlabSize(size_class_data, begin, end, pages);

            size_t start = randomSize();
            size_t stop = randomSize();
            size_t num_shards = size_t(next() % 70);
            bool ref_error = ref_bin_update_shard_size(start, stop, num_shards);
            bool own_error = binUpdateShardSize(shards, start, stop, num_shards);
            CHECK_EQ(own_error, ref_error);
        }
        ref_bin_info_boot();
        binInfoBoot(size_class_data, shards);
        compareSizeClassData();
        compareBinInfos();
        REQUIRE(allocator_test::failureCount() < 100);
    }
    bootBoth(true);
}
