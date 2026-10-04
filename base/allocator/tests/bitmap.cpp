/// Checks both bitmap layouts (flat and tree, regardless of which one the page size selects) against a trivial model:
/// `sfu` returns the lowest free bit, `ffu` the lowest free bit >= the argument, the physical representation is
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
    size_t nbits = info.nbits;
    /// Leaf level: inverted bits, unused high bits are 0.
    for (size_t g = 0; g < bitmapBitsToGroups(nbits); ++g)
    {
        bitmap_t expected = 0;
        for (size_t b = 0; b < 64 && g * 64 + b < nbits; ++b)
            if (!model[g * 64 + b])
                expected |= bitmap_t(1) << b;
        CHECK_EQ(bitmap[g], expected);
    }
    if constexpr (UseTree)
    {
        for (unsigned level = 1; level < info.nlevels; ++level)
        {
            size_t child_offset = info.levels[level - 1].group_offset;
            size_t nchildren = info.levels[level].group_offset - child_offset;
            for (size_t g = 0; g < bitmapBitsToGroups(nchildren); ++g)
            {
                bitmap_t expected = 0;
                for (size_t b = 0; b < 64 && g * 64 + b < nchildren; ++b)
                    if (bitmap[child_offset + g * 64 + b] != 0)
                        expected |= bitmap_t(1) << b;
                CHECK_EQ(bitmap[info.levels[level].group_offset + g], expected);
            }
        }
    }
}

size_t modelFfu(const std::vector<bool> & model, size_t min_bit)
{
    for (size_t i = min_bit; i < model.size(); ++i)
        if (!model[i])
            return i;
    return model.size();
}

template <bool UseTree>
void runTrace(size_t nbits, uint64_t seed)
{
    BitmapInfoImpl<UseTree> info = bitmapInfoInitializer<UseTree>(nbits);
    BitmapInfoImpl<UseTree> info2;
    bitmapInfoInit(info2, nbits);
    CHECK_EQ(bitmapSize(info), bitmapSize(info2));
    CHECK_EQ(bitmapSize(info), bitmapInfoNumGroups(info) * 8);
    if constexpr (UseTree)
    {
        CHECK_EQ(info.nlevels, info2.nlevels);
        for (unsigned l = 0; l <= info.nlevels; ++l)
            CHECK_EQ(info.levels[l].group_offset, info2.levels[l].group_offset);
    }
    else
        CHECK_EQ(info.ngroups, bitmapBitsToGroups(nbits));

    size_t ngroups = bitmapInfoNumGroups(info);
    std::vector<bitmap_t> bitmap(ngroups + 1, 0x5a5a5a5a5a5a5a5aUL);
    std::vector<bool> model(nbits, true);

    bitmapInit(bitmap.data(), info, true);
    for (size_t g = 0; g < ngroups; ++g)
        CHECK_EQ(bitmap[g], 0u);
    CHECK(bitmapFull(bitmap.data(), info));

    bitmapInit(bitmap.data(), info, false);
    CHECK_EQ(bitmap[ngroups], 0x5a5a5a5a5a5a5a5aUL);
    model.assign(nbits, false);
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
    int steps = int(nbits * 4 + 1000);
    for (int step = 0; step < steps; ++step)
    {
        int phase = (step / int(nbits + 20)) % 3;
        uint64_t r = next();
        unsigned alloc_percent = phase == 0 ? 85 : (phase == 1 ? 15 : 50);
        bool full = allocated.size() == nbits;
        CHECK_EQ(bitmapFull(bitmap.data(), info), full);

        if (!full && (allocated.empty() || r % 100 < alloc_percent))
        {
            size_t bit;
            if (r & (1 << 20))
            {
                bit = bitmapSfu(bitmap.data(), info);
                CHECK_EQ(bit, modelFfu(model, 0));
            }
            else
            {
                size_t min_bit = size_t(next() % nbits);
                bit = bitmapFfu(bitmap.data(), info, min_bit);
                CHECK_EQ(bit, modelFfu(model, min_bit));
                if (bit == nbits)
                    bit = modelFfu(model, 0);
                bitmapSet(bitmap.data(), info, bit);
            }
            REQUIRE(bit < nbits && !model[bit]);
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
        if (step % 7 == 0 || nbits < 300)
            checkInvariants(bitmap, info, model);
        size_t bit = size_t(next() % nbits);
        CHECK_EQ(bitmapGet(bitmap.data(), info, bit), bool(model[bit]));
        CHECK_EQ(bitmap[ngroups], 0x5a5a5a5a5a5a5a5aUL);
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
    for (unsigned i = 0; i < SC_NBINS; ++i)
        counts.push_back(bin_infos[i].nregs);
    for (size_t n : counts)
    {
        if (n > BITMAP_MAXBITS)
            continue;
        runTrace<UseTree>(n, seed);
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    }
}

}

TEST(Bitmap, Constants)
{
    static_assert(BITMAP_USE_TREE == (LG_PAGE != 12));
    static_assert(BITMAP_GROUPS_MAX == (LG_PAGE == 12 ? 8 : (LG_PAGE == 14 ? 33 : 131)));
    static_assert(LG_BITMAP_MAXBITS == LG_PAGE - 3);

    constexpr BitmapInfoImpl<true> tree = bitmapInfoInitializer<true>(8192);
    static_assert(tree.nbits == 8192 && tree.nlevels == 3);
    static_assert(tree.levels[0].group_offset == 0 && tree.levels[1].group_offset == 128 && tree.levels[2].group_offset == 130
        && tree.levels[3].group_offset == 131 && tree.levels[4].group_offset == 132 && tree.levels[5].group_offset == 133);
    constexpr BitmapInfoImpl<true> one = bitmapInfoInitializer<true>(64);
    static_assert(one.nlevels == 1 && one.levels[1].group_offset == 1);
    constexpr BitmapInfoImpl<false> flat = bitmapInfoInitializer<false>(512);
    static_assert(flat.nbits == 512 && flat.ngroups == 8);
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
