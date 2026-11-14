/// Checks both bitmap layouts (flat and tree, regardless of which one the page size selects) against a trivial model:
/// `sfu` returns the lowest free bit, `findFirstUnset` the lowest free bit >= the argument, the physical representation is
/// inverted, and the upper tree levels summarize the lower ones.

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <vector>

using namespace jemalloc;

namespace
{

template <bool UseTree>
void checkInvariants(const std::vector<bitmap_t> & bitmap, const BitmapInfoImpl<UseTree> & info, const std::vector<bool> & model)
{
    size_t num_bits = info.num_bits;
    /// Leaf level: inverted bits, unused high bits are 0.
    for (size_t g = 0; g < bitmapBitsToGroups(num_bits); ++g)
    {
        bitmap_t expected = 0;
        for (size_t b = 0; b < 64 && g * 64 + b < num_bits; ++b)
            if (!model[g * 64 + b])
                expected |= bitmap_t(1) << b;
        CHECK_EQ(bitmap[g], expected);
    }
    if constexpr (UseTree)
    {
        for (unsigned level = 1; level < info.num_levels; ++level)
        {
            size_t child_offset = info.levels[level - 1].group_offset;
            size_t num_children = info.levels[level].group_offset - child_offset;
            for (size_t g = 0; g < bitmapBitsToGroups(num_children); ++g)
            {
                bitmap_t expected = 0;
                for (size_t b = 0; b < 64 && g * 64 + b < num_children; ++b)
                    if (bitmap[child_offset + g * 64 + b] != 0)
                        expected |= bitmap_t(1) << b;
                CHECK_EQ(bitmap[info.levels[level].group_offset + g], expected);
            }
        }
    }
}

size_t modelFindFirstUnset(const std::vector<bool> & model, size_t min_bit)
{
    for (size_t i = min_bit; i < model.size(); ++i)
        if (!model[i])
            return i;
    return model.size();
}

template <bool UseTree>
void runTrace(size_t num_bits, uint64_t seed)
{
    BitmapInfoImpl<UseTree> info = bitmapInfoInitializer<UseTree>(num_bits);
    BitmapInfoImpl<UseTree> info2;
    bitmapInfoInit(info2, num_bits);
    CHECK_EQ(bitmapSize(info), bitmapSize(info2));
    CHECK_EQ(bitmapSize(info), bitmapInfoNumGroups(info) * 8);
    if constexpr (UseTree)
    {
        CHECK_EQ(info.num_levels, info2.num_levels);
        for (unsigned l = 0; l <= info.num_levels; ++l)
            CHECK_EQ(info.levels[l].group_offset, info2.levels[l].group_offset);
    }
    else
        CHECK_EQ(info.num_groups, bitmapBitsToGroups(num_bits));

    size_t num_groups = bitmapInfoNumGroups(info);
    std::vector<bitmap_t> bitmap(num_groups + 1, 0x5a5a5a5a5a5a5a5aUL);
    std::vector<bool> model(num_bits, true);

    bitmapInit(bitmap.data(), info, true);
    for (size_t g = 0; g < num_groups; ++g)
        CHECK_EQ(bitmap[g], 0u);
    CHECK(bitmapFull(bitmap.data(), info));

    bitmapInit(bitmap.data(), info, false);
    CHECK_EQ(bitmap[num_groups], 0x5a5a5a5a5a5a5a5aUL);
    model.assign(num_bits, false);
    checkInvariants(bitmap, info, model);
    CHECK(!bitmapFull(bitmap.data(), info));

    std::vector<size_t> allocated;
    uint64_t x = seed;
    auto next = [&x]
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    };
    int steps = int(num_bits * 4 + 1000);
    for (int step = 0; step < steps; ++step)
    {
        int phase = (step / int(num_bits + 20)) % 3;
        uint64_t r = next();
        unsigned alloc_percent = phase == 0 ? 85 : (phase == 1 ? 15 : 50);
        bool full = allocated.size() == num_bits;
        CHECK_EQ(bitmapFull(bitmap.data(), info), full);

        if (!full && (allocated.empty() || r % 100 < alloc_percent))
        {
            size_t bit;
            if (r & (1 << 20))
            {
                bit = bitmapSetFirstUnset(bitmap.data(), info);
                CHECK_EQ(bit, modelFindFirstUnset(model, 0));
            }
            else
            {
                size_t min_bit = size_t(next() % num_bits);
                bit = bitmapFindFirstUnset(bitmap.data(), info, min_bit);
                CHECK_EQ(bit, modelFindFirstUnset(model, min_bit));
                if (bit == num_bits)
                    bit = modelFindFirstUnset(model, 0);
                bitmapSet(bitmap.data(), info, bit);
            }
            REQUIRE(bit < num_bits && !model[bit]);
            model[bit] = true;
            allocated.push_back(bit);
        }
        else if (!allocated.empty())
        {
            size_t pos = size_t(next() % allocated.size());
            size_t bit = allocated[pos];
            allocated[pos] = allocated.back();
            allocated.pop_back();
            bitmapUnset(bitmap.data(), info, bit);
            model[bit] = false;
        }
        if (step % 7 == 0 || num_bits < 300)
            checkInvariants(bitmap, info, model);
        size_t bit = size_t(next() % num_bits);
        CHECK_EQ(bitmapGet(bitmap.data(), info, bit), bool(model[bit]));
        CHECK_EQ(bitmap[num_groups], 0x5a5a5a5a5a5a5a5aUL);
        REQUIRE(allocator_test::failureCount() < 100);
    }
}

template <bool UseTree>
void runAll()
{
    uint64_t seed = 0xDEADBEEFCAFEBABEULL;
    std::vector<size_t> counts;
    for (size_t n = 1; n <= 200; ++n)
        counts.push_back(n);
    for (size_t n : {255, 256, 257, 511, 512, 1000, 2047, 2048, 2049, 4095, 4096, 4097, 4160, 4161, 8191, 8192})
        counts.push_back(n);
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        counts.push_back(bin_infos[i].num_regions);
    for (size_t n : counts)
    {
        if (n > BITMAP_MAX_BITS)
            continue;
        runTrace<UseTree>(n, seed);
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    }
}

}

TEST(Bitmap, Constants)
{
    static_assert(BITMAP_USE_TREE == (LOG2_PAGE != 12));
    static_assert(BITMAP_GROUPS_MAX == (LOG2_PAGE == 12 ? 8 : (LOG2_PAGE == 14 ? 33 : 131)));
    static_assert(LOG2_BITMAP_MAX_BITS == LOG2_PAGE - 3);

    constexpr BitmapInfoImpl<true> tree = bitmapInfoInitializer<true>(8192);
    static_assert(tree.num_bits == 8192 && tree.num_levels == 3);
    static_assert(
        tree.levels[0].group_offset == 0 && tree.levels[1].group_offset == 128 && tree.levels[2].group_offset == 130
        && tree.levels[3].group_offset == 131 && tree.levels[4].group_offset == 132 && tree.levels[5].group_offset == 133);
    constexpr BitmapInfoImpl<true> one = bitmapInfoInitializer<true>(64);
    static_assert(one.num_levels == 1 && one.levels[1].group_offset == 1);
    constexpr BitmapInfoImpl<false> flat = bitmapInfoInitializer<false>(512);
    static_assert(flat.num_bits == 512 && flat.num_groups == 8);
    CHECK(true);
}

TEST(Bitmap, Flat)
{
    runAll<false>();
}

TEST(Bitmap, Tree)
{
    runAll<true>();
}
