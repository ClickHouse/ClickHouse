#include <Compression/FFOR.h>

#include <random>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
template <typename T, UInt16 values>
void runFFORPackUnpackTest(UInt8 bits)
{
    if (bits > sizeof(T) * 8)
        GTEST_SKIP() << "Skipping invalid bit width for " << typeid(T).name() << ": " << static_cast<UInt32>(bits);

    alignas(64) T in[values];
    alignas(64) T coded[values];
    alignas(64) T decoded[values];

    // Generate random input data
    const T max_value = (T{1} << std::min<UInt16>(bits, sizeof(T) * 8 - 1)) - T{1};
    std::default_random_engine rng(T{42}); // NOLINT
    std::uniform_int_distribution<T> in_dist(T{0}, max_value);
    T base = in_dist(rng);
    for (auto & i : in)
        i = base + in_dist(rng);

    // Encode
    Compression::FFOR::bitPack<values>(in, coded, bits, base);

    // Set unused bytes to random values to ensure decoder does not rely on them
    const UInt16 used_bytes = Compression::FFOR::calculateBitpackedBytes<values>(bits);
    std::uniform_int_distribution<UInt16> byte_dist(0, 255);
    char * coded_bytes = reinterpret_cast<char *>(coded);
    for (UInt32 i = used_bytes; i < sizeof(coded); ++i)
        coded_bytes[i] = static_cast<char>(byte_dist(rng));

    // Decode
    Compression::FFOR::bitUnpack<values>(coded, decoded, bits, base);

    // Verify
    for (UInt16 i = 0; i < values; ++i)
        ASSERT_EQ(decoded[i], in[i]) << "bits=" << static_cast<UInt32>(bits) << " index=" << i;
}

class FFORTest : public ::testing::TestWithParam<UInt8> { };

TEST_P(FFORTest, UInt16PackUnpack1024Values)
{
    runFFORPackUnpackTest<UInt16, 1024>(GetParam());
}

TEST_P(FFORTest, UInt16PackUnpack2048Values)
{
    runFFORPackUnpackTest<UInt16, 2048>(GetParam());
}

TEST_P(FFORTest, UInt32PackUnpack1024Values)
{
    runFFORPackUnpackTest<UInt32, 1024>(GetParam());
}

TEST_P(FFORTest, UInt32PackUnpack2048Values)
{
    runFFORPackUnpackTest<UInt32, 2048>(GetParam());
}

TEST_P(FFORTest, UInt64PackUnpack1024Values)
{
    runFFORPackUnpackTest<UInt64, 1024>(GetParam());
}

TEST_P(FFORTest, UInt64PackUnpack2048Values)
{
    runFFORPackUnpackTest<UInt64, 2048>(GetParam());
}

INSTANTIATE_TEST_SUITE_P(FFORTest, FFORTest, ::testing::Range<UInt8>(0, 65));

}
