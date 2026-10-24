/// Compares `Bitmap.h` with jemalloc's `bitmap.h`/`bitmap.c` (`bitmap_oracle_ref.c`): the bitmap info for every
/// bit count, and the complete bitmap contents plus the results of `sfu`/`findFirstUnset`/`get`/`full` after every operation of
/// long randomized traces, for the bit counts of all bins and a few others.

#include <allocator/SizeClasses.h>

#include "Test.h"

#include <vector>

using namespace jemalloc;

extern "C" {
int ref_bitmap_use_tree();
size_t ref_bitmap_groups_max();
size_t ref_bitmap_max_bits();
void ref_bitmap_info_initializer(size_t num_bits, size_t * out);
void ref_bitmap_info_init(size_t num_bits, size_t * out);
void ref_bitmap_select(size_t num_bits);
size_t ref_bitmap_size();
void ref_bitmap_init(bitmap_t * bitmap, bool fill);
bool ref_bitmap_full(bitmap_t * bitmap);
bool ref_bitmap_get(bitmap_t * bitmap, size_t bit);
void ref_bitmap_set(bitmap_t * bitmap, size_t bit);
void ref_bitmap_unset(bitmap_t * bitmap, size_t bit);
size_t ref_bitmap_set_first_unset(bitmap_t * bitmap);
size_t ref_bitmap_find_first_unset(bitmap_t * bitmap, size_t min_bit);
}

namespace
{

/// A template, so that the tree branch is not checked with a flat bitmap (`LOG2_PAGE` 12).
template <typename Info>
void infoFields(const Info & info, size_t * out, bool all_levels)
{
    out[0] = info.num_bits;
    if constexpr (BITMAP_USE_TREE)
    {
        out[1] = info.num_levels;
        unsigned num_levels = all_levels ? BITMAP_MAX_LEVELS : info.num_levels;
        for (unsigned l = 0; l <= num_levels; ++l)
            out[2 + l] = info.levels[l].group_offset;
    }
    else
        out[1] = bitmapInfoNumGroups(info);
}

struct Rng
{
    uint64_t x;

    uint64_t next()
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x;
    }
};

bool compareContents(const std::vector<bitmap_t> & own, const std::vector<bitmap_t> & ref, size_t num_groups, size_t num_bits, int step)
{
    for (size_t g = 0; g < num_groups; ++g)
    {
        if (own[g] != ref[g])
        {
            std::fprintf(stderr, "nbits %zu, step %d: group %zu differs: %lx vs %lx\n", num_bits, step, g, own[g], ref[g]);
            ++allocator_test::failureCount();
            return false;
        }
    }
    return true;
}

void runTrace(size_t num_bits, uint64_t seed)
{
    const BitmapInfo info = bitmapInfoInitializer(num_bits);
    ref_bitmap_select(num_bits);
    size_t size = bitmapSize(info);
    REQUIRE(size == ref_bitmap_size());
    REQUIRE(size <= BITMAP_GROUPS_MAX * sizeof(bitmap_t));
    size_t num_groups = size / sizeof(bitmap_t);

    /// One extra guard group to detect writes past the end.
    std::vector<bitmap_t> own(num_groups + 1, 0x5a5a5a5a5a5a5a5aUL);
    std::vector<bitmap_t> ref(num_groups + 1, 0x5a5a5a5a5a5a5a5aUL);

    bitmapInit(own.data(), info, true);
    ref_bitmap_init(ref.data(), true);
    compareContents(own, ref, num_groups + 1, num_bits, -2);
    CHECK(bitmapFull(own.data(), info));

    bitmapInit(own.data(), info, false);
    ref_bitmap_init(ref.data(), false);
    compareContents(own, ref, num_groups + 1, num_bits, -1);
    CHECK_EQ(bitmapFull(own.data(), info), ref_bitmap_full(ref.data()));

    std::vector<size_t> allocated;
    Rng rng{seed};
    int steps = int(num_bits * 6 + 3000);
    for (int step = 0; step < steps; ++step)
    {
        /// Phases: mostly allocating, mostly freeing, mixed.
        int phase = (step / int(num_bits + 50)) % 3;
        uint64_t r = rng.next();
        unsigned alloc_percent = phase == 0 ? 85 : (phase == 1 ? 15 : 50);
        bool full = bitmapFull(own.data(), info);
        CHECK_EQ(full, ref_bitmap_full(ref.data()));

        if (!full && (allocated.empty() || r % 100 < alloc_percent))
        {
            if (r & (1 << 20))
            {
                size_t own_bit = bitmapSetFirstUnset(own.data(), info);
                size_t ref_bit = ref_bitmap_set_first_unset(ref.data());
                CHECK_EQ(own_bit, ref_bit);
                allocated.push_back(own_bit);
            }
            else
            {
                /// Set a specific free bit (found with ffu from a random position).
                size_t min_bit = size_t(rng.next() % num_bits);
                size_t own_bit = bitmapFindFirstUnset(own.data(), info, min_bit);
                size_t ref_bit = ref_bitmap_find_first_unset(ref.data(), min_bit);
                CHECK_EQ(own_bit, ref_bit);
                if (own_bit == num_bits)
                    own_bit = bitmapFindFirstUnset(own.data(), info, 0);
                REQUIRE(own_bit < num_bits);
                bitmapSet(own.data(), info, own_bit);
                ref_bitmap_set(ref.data(), own_bit);
                allocated.push_back(own_bit);
            }
        }
        else if (!allocated.empty())
        {
            size_t pos = size_t(rng.next() % allocated.size());
            size_t bit = allocated[pos];
            allocated[pos] = allocated.back();
            allocated.pop_back();
            bitmapUnset(own.data(), info, bit);
            ref_bitmap_unset(ref.data(), bit);
        }

        if (!compareContents(own, ref, num_groups + 1, num_bits, step))
            return;

        /// Query a few random positions.
        for (int q = 0; q < 3; ++q)
        {
            size_t bit = size_t(rng.next() % num_bits);
            CHECK_EQ(bitmapGet(own.data(), info, bit), ref_bitmap_get(ref.data(), bit));
            CHECK_EQ(bitmapFindFirstUnset(own.data(), info, bit), ref_bitmap_find_first_unset(ref.data(), bit));
        }
        REQUIRE(allocator_test::failureCount() < 100);
    }

    /// Exhaustive queries at the end of the trace, and drain it with sfu.
    for (size_t bit = 0; bit < num_bits; ++bit)
    {
        CHECK_EQ(bitmapGet(own.data(), info, bit), ref_bitmap_get(ref.data(), bit));
        CHECK_EQ(bitmapFindFirstUnset(own.data(), info, bit), ref_bitmap_find_first_unset(ref.data(), bit));
    }
    while (!ref_bitmap_full(ref.data()))
    {
        REQUIRE(!bitmapFull(own.data(), info));
        CHECK_EQ(bitmapSetFirstUnset(own.data(), info), ref_bitmap_set_first_unset(ref.data()));
    }
    CHECK(bitmapFull(own.data(), info));
    compareContents(own, ref, num_groups + 1, num_bits, steps);
}

std::vector<size_t> bitCounts()
{
    std::vector<size_t> counts;
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        counts.push_back(bin_infos[i].num_regions);
    for (size_t n = 1; n <= 130; ++n)
        counts.push_back(n);
    for (size_t n :
         {191, 192, 193, 255, 256, 257, 511, 512, 513, 1000, 2047, 2048, 2049, 4095, 4096, 4097, 4159, 4160, 4161, 5000, 8000, 8191, 8192})
        if (n <= BITMAP_MAX_BITS)
            counts.push_back(n);
    return counts;
}

}

TEST(BitmapOracle, Constants)
{
    CHECK_EQ(ref_bitmap_use_tree(), int(BITMAP_USE_TREE));
    CHECK_EQ(ref_bitmap_groups_max(), BITMAP_GROUPS_MAX);
    CHECK_EQ(ref_bitmap_max_bits(), BITMAP_MAX_BITS);
}

TEST(BitmapOracle, Info)
{
    for (size_t num_bits = 1; num_bits <= BITMAP_MAX_BITS; ++num_bits)
    {
        size_t own[8] = {};
        size_t ref[8] = {};
        infoFields(bitmapInfoInitializer(num_bits), own, true);
        ref_bitmap_info_initializer(num_bits, ref);
        for (unsigned k = 0; k < 8; ++k)
            CHECK_EQ(own[k], ref[k]);

        size_t own_init[8] = {};
        size_t ref_init[8] = {};
        BitmapInfo info;
        bitmapInfoInit(info, num_bits);
        infoFields(info, own_init, false);
        ref_bitmap_info_init(num_bits, ref_init);
        for (unsigned k = 0; k < 8; ++k)
            CHECK_EQ(own_init[k], ref_init[k]);

        ref_bitmap_select(num_bits);
        CHECK_EQ(bitmapSize(info), ref_bitmap_size());
        CHECK_EQ(bitmapSize(bitmapInfoInitializer(num_bits)), ref_bitmap_size());
        REQUIRE(allocator_test::failureCount() < 100);
    }
}

TEST(BitmapOracle, RandomizedTraces)
{
    uint64_t seed = 0x123456789ABCDEFULL;
    for (size_t num_bits : bitCounts())
    {
        runTrace(num_bits, seed);
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
    }
}
