#include <Interpreters/BloomFilter.h>

#include <gtest/gtest.h>

#include <random>
#include <vector>

using namespace DB;

namespace
{
std::vector<BloomFilterHashPair> makeRandomPairs(size_t size)
{
    std::mt19937_64 rng(42);
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
