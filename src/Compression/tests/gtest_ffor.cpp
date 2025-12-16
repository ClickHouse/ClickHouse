#include <Compression/FFOR.h>

#include <random>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
template <typename T>
void runFFORPackUnpackTest(UInt16 bits)
{
    alignas(64) T in[Compression::FFOR::DEFAULT_VALUES];
    alignas(64) T coded[Compression::FFOR::DEFAULT_VALUES];
    alignas(64) T decoded[Compression::FFOR::DEFAULT_VALUES];

    // Generate random input data
    const T max_value = (T{1} << std::min<UInt16>(bits, sizeof(T) * 8u - 1)) - T{1};
    std::default_random_engine rng(0xC0FFEEULL); // Fixed seed for reproducibility
    std::uniform_int_distribution<T> dist(0, max_value);
    T base = dist(rng);
    for (auto & i : in)
        i = base + dist(rng);

    // Encode
    Compression::FFOR::bitPack(in, coded, bits, base);

    // Set unused bits to zero to ensure decoder does not rely on them
    const UInt16 used_bytes = Compression::FFOR::calculateBitpackedBytes(bits);
    std::memset(reinterpret_cast<char*>(coded) + used_bytes, 0, sizeof(coded) - used_bytes);

    // Decode
    Compression::FFOR::bitUnpack(coded, decoded, bits, base);

    // Verify
    for (UInt16 i = 0; i < Compression::FFOR::DEFAULT_VALUES; ++i)
        ASSERT_EQ(decoded[i], in[i]) << "bits=" << bits << " index=" << i;
}

class FFORTest : public ::testing::TestWithParam<UInt16> { };

TEST_P(FFORTest, UInt32PackUnpack)
{
    const UInt16 bits = GetParam();
    if (bits > 32)
        GTEST_SKIP() << "Skipping invalid bit width for UInt32: " << bits;

    runFFORPackUnpackTest<UInt32>(bits);
}

TEST_P(FFORTest, UInt64PackUnpack)
{
    runFFORPackUnpackTest<UInt64>(GetParam());
}

INSTANTIATE_TEST_SUITE_P(FFORTest, FFORTest, ::testing::Range<UInt16>(0, 65));

}
