#include <Interpreters/BloomFilter.h>

#include <gtest/gtest.h>

#include <pcg_random.hpp>

#include <limits>
#include <random>
#include <string_view>
#include <vector>

using namespace DB;

namespace
{
std::vector<BloomFilterHashPair> makeRandomPairs(size_t size)
{
    pcg64 rng(42);
    std::uniform_int_distribution<UInt64> dist;

    std::vector<BloomFilterHashPair> pairs(size);
    for (size_t i = 0; i < size; ++i)
        pairs[i] = {dist(rng), dist(rng)};
    return pairs;
}
}

TEST(BloomFilter, BatchPrehashedParity)
{
    constexpr size_t bloom_size = 4096;
    constexpr UInt64 bloom_seed = 42;
    constexpr size_t pair_count = 256;

    const auto pairs = makeRandomPairs(pair_count);

    for (size_t hashes : {size_t{1}, size_t{3}, size_t{5}, size_t{10}})
    {
        BloomFilter scalar_filter(bloom_size, hashes, bloom_seed);
        BloomFilter batch_filter(bloom_size, hashes, bloom_seed);

        for (const auto & pair : pairs)
            scalar_filter.addHashPair(pair);

        batch_filter.addHashPairs(pairs.data(), pairs.size());

        EXPECT_EQ(scalar_filter, batch_filter);

        std::vector<UInt8> batch_mask(pair_count);
        const size_t batch_found = batch_filter.findHashPairs(pairs.data(), pairs.size(), batch_mask.data());

        size_t scalar_found = 0;
        for (size_t i = 0; i < pair_count; ++i)
        {
            const bool scalar_hit = scalar_filter.findHashPair(pairs[i]);
            scalar_found += scalar_hit;
            EXPECT_EQ(static_cast<bool>(batch_mask[i]), scalar_hit);
        }

        EXPECT_EQ(batch_found, scalar_found);
    }
}

TEST(BloomFilter, ComputeHashPairGoldenValues)
{
    /// Text bloom filter indexes persist bits derived from these values, so they must not change.
    struct Case
    {
        std::string_view data;
        UInt64 seed;
        UInt64 hash1;
        UInt64 hash2;
    };
    const std::vector<Case> cases{
        {"", 0, 0ULL, 167021372700187220ULL},
        {"", std::numeric_limits<UInt64>::max(), 10693559443859979498ULL, 11099807243730275057ULL},
        {"ClickHouse", 0, 17504870580740013348ULL, 13551335315504348687ULL},
        {"ClickHouse", std::numeric_limits<UInt64>::max(), 3955525648307601364ULL, 9038073283059794754ULL},
    };

    for (const auto & c : cases)
    {
        const auto pair = BloomFilter::computeHashPair(c.data.data(), c.data.size(), c.seed);
        EXPECT_EQ(pair.hash1, c.hash1) << "data='" << c.data << "' seed=" << c.seed;
        EXPECT_EQ(pair.hash2, c.hash2) << "data='" << c.data << "' seed=" << c.seed;
    }
}
