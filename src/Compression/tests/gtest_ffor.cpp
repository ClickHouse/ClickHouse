#include <Compression/FFOR.h>

#include <random>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
template <typename T, UInt16 values>
using InputGeneratorFn = std::function<T(T * in, UInt8 bits)>;

template <typename T>
T calculateMaxValue(T min, UInt8 bits)
{
    constexpr UInt8 max_bits = sizeof(T) * 8;
    return bits == max_bits ? ~T{0} : static_cast<T>((T{1} << bits) - T{1} + min);
}

template <typename T, UInt16 values = Compression::FFOR::DEFAULT_VALUES>
T generateRandomInput(T * in, UInt8 bits)
{
    T min = T{5};
    T max = calculateMaxValue(min, bits);

    std::default_random_engine rng(17); // NOLINT
    std::uniform_int_distribution<T> in_dist(min, max);
    for (UInt16 i = 0; i < values; ++i)
        in[i] = in_dist(rng);

    return min;
}

template <typename T, UInt16 values = Compression::FFOR::DEFAULT_VALUES>
T generateMinMaxInput(T * in, UInt8 bits)
{
    T min = T{0};
    T max = calculateMaxValue(min, bits);

    for (UInt16 i = 0; i < values; ++i)
        in[i] = i % 2 == 0 ? max : (i % 3 == 0 ? max : min);

    return min;
}

template <typename T, UInt16 values = Compression::FFOR::DEFAULT_VALUES>
void runPackUnpackCorrectnessTest(UInt8 bits, InputGeneratorFn<T, values> generator)
{
    alignas(64) T in[values];
    alignas(64) T coded[values];
    alignas(64) T decoded[values];

    // Generate input data using the provided generator
    T base = generator(in, bits);

    // Encode
    Compression::FFOR::bitPack<values>(in, coded, bits, base);

    // Set unused bytes to random values to ensure decoder does not rely on them
    const UInt16 used_bytes = Compression::FFOR::calculateBitpackedBytes<values>(bits);
    UInt16 total_bytes = sizeof(coded);
    char * coded_bytes = reinterpret_cast<char *>(coded);
    std::default_random_engine rng(13); // NOLINT
    std::uniform_int_distribution<UInt16> byte_dist(0, 255);
    for (auto i = used_bytes; i < total_bytes; ++i)
        coded_bytes[i] = static_cast<char>(byte_dist(rng));

    // Decode
    Compression::FFOR::bitUnpack<values>(coded, decoded, bits, base);

    // Verify
    for (UInt16 i = 0; i < values; ++i)
        ASSERT_EQ(decoded[i], in[i]) << "bits=" << static_cast<UInt32>(bits) << " index=" << i;
}

class FFOR16BitTest : public ::testing::TestWithParam<UInt8> { };
class FFOR32BitTest : public ::testing::TestWithParam<UInt8> { };
class FFOR64BitTest : public ::testing::TestWithParam<UInt8> { };

TEST_P(FFOR16BitTest, RandomPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt16>(GetParam(), generateRandomInput<UInt16>);
}

TEST_P(FFOR16BitTest, MinMaxPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt16>(GetParam(), generateMinMaxInput<UInt16>);
}

TEST_P(FFOR32BitTest, RandomPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt32>(GetParam(), generateRandomInput<UInt32>);
}

TEST_P(FFOR32BitTest, MinMaxPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt32>(GetParam(), generateMinMaxInput<UInt32>);
}

TEST_P(FFOR64BitTest, RandomPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt64>(GetParam(), generateRandomInput<UInt64>);
}

TEST_P(FFOR64BitTest, MinMaxPackUnpackCorrectnessTest)
{
    runPackUnpackCorrectnessTest<UInt64>(GetParam(), generateMinMaxInput<UInt64>);
}

INSTANTIATE_TEST_SUITE_P(FFOR, FFOR16BitTest, ::testing::Range<UInt8>(0, sizeof(UInt16) * 8 + 1));
INSTANTIATE_TEST_SUITE_P(FFOR, FFOR32BitTest, ::testing::Range<UInt8>(0, sizeof(UInt32) * 8 + 1));
INSTANTIATE_TEST_SUITE_P(FFOR, FFOR64BitTest, ::testing::Range<UInt8>(0, sizeof(UInt64) * 8 + 1));

}
