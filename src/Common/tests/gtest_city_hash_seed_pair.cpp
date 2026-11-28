#include <city.h>

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

/// CityHash64WithSeedPair must stay bit-identical to two CityHash64WithSeed calls: BloomFilter::computeHashPair uses it, and
/// text bloom filter indexes (tokenbf_v1, ngrambf_v1, sparse_grams) persist bits derived from it.
TEST(CityHash, SeedPairEqualsTwoSeededCalls)
{
    /// Lengths around every branch boundary of CityHash64 (<=16, <=32, <=64, >64 loop) plus
    /// the 64-byte loop's tail cases.
    const std::vector<size_t> lengths{
        0, 1, 2, 3, 4, 7, 8, 9, 12, 15, 16, 17, 20, 23, 24, 31, 32, 33, 40, 48, 63, 64, 65, 66,
        79, 80, 95, 96, 127, 128, 129, 191, 192, 255, 256, 257, 1000, 1024, 4097};

    const std::vector<uint64_t> seeds{
        0, 1, 2, 3, 0x9ae16a3b2f90404fULL, 0xc949d7c7509e6557ULL, 0xb492b66fbe98f273ULL,
        0x2b9c4e0a9e0b1e19ULL, static_cast<uint64_t>(-1), static_cast<uint64_t>(-2)};

    std::string data;
    data.reserve(4200);
    for (size_t i = 0; i < 4200; ++i)
        data.push_back(static_cast<char>((i * 167 + (i >> 3)) & 0xFF));

    for (size_t len : lengths)
    {
        const char * buf = data.data();
        for (uint64_t seed0 : seeds)
        {
            for (uint64_t seed1 : seeds)
            {
                const auto pair = CityHash_v1_0_2::CityHash64WithSeedPair(buf, len, seed0, seed1);
                EXPECT_EQ(pair.first, CityHash_v1_0_2::CityHash64WithSeed(buf, len, seed0))
                    << "len=" << len << " seed0=" << seed0;
                EXPECT_EQ(pair.second, CityHash_v1_0_2::CityHash64WithSeed(buf, len, seed1))
                    << "len=" << len << " seed1=" << seed1;
            }
        }
        /// Offset the buffer so alignment-dependent paths are exercised too.
        for (size_t offset : {size_t(1), size_t(3), size_t(7)})
        {
            const auto pair = CityHash_v1_0_2::CityHash64WithSeedPair(buf + offset, len, 7, 11);
            EXPECT_EQ(pair.first, CityHash_v1_0_2::CityHash64WithSeed(buf + offset, len, 7));
            EXPECT_EQ(pair.second, CityHash_v1_0_2::CityHash64WithSeed(buf + offset, len, 11));
        }
    }
}
