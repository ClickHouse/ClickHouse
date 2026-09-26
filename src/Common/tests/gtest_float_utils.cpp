#include <gtest/gtest.h>

#include <Common/FloatUtils.h>

#include <fmt/format.h>

#include <bit>
#include <cmath>
#include <cstdint>

namespace
{

uint32_t asBits(float x)
{
    return std::bit_cast<uint32_t>(x);
}

float fromBits(uint32_t bits)
{
    return std::bit_cast<float>(bits);
}

::testing::AssertionResult assertBitsEqual(const char * actual_expr, const char * expected_expr, uint32_t actual, uint32_t expected)
{
    if (actual == expected)
        return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure() << fmt::format(
               "\n  Actual: {} = 0x{:08x}\nExpected: {} = 0x{:08x}", actual_expr, actual, expected_expr, expected);
}

}

#define EXPECT_BITS_EQ(actual, expected) EXPECT_PRED_FORMAT2(assertBitsEqual, actual, expected)

TEST(FloatUtils, Zeros)
{
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x0000)), 0x00000000u);
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x8000)), 0x80000000u);
}

TEST(FloatUtils, SubnormalBoundaries)
{
    /// Smallest positive subnormal: 2^-24
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x0001)), asBits(fromBits(0x33800000)));

    /// Power-of-two subnormal: 2^-15 (the "remainder == 0" path that happens
    /// to work even with an off-by-one mantissa shift)
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x0200)), asBits(fromBits(0x38000000)));

    /// Largest subnormal — regression for issue #104831
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x03FF)), asBits(fromBits(0x387FC000)));

    /// Negative largest subnormal
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x83FF)), asBits(fromBits(0xB87FC000)));

    /// Mid-range subnormal with leading mantissa bit set: 514 * 2^-24
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x0202)), asBits(fromBits(0x38008000)));
}

TEST(FloatUtils, AllSubnormalsExact)
{
    /// Every float16 subnormal must convert to exactly M * 2^-24 in float32
    for (uint32_t m = 1; m < 1024; ++m)
    {
        const auto bits = static_cast<uint16_t>(m);
        const float expected = static_cast<float>(m) / static_cast<float>(1u << 24);
        EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(bits)), asBits(expected)) << "mantissa " << m;

        const auto neg_bits = static_cast<uint16_t>(0x8000u | m);
        EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(neg_bits)), asBits(-expected)) << "mantissa " << m << " (neg)";
    }
}

TEST(FloatUtils, NormalBoundaries)
{
    /// Smallest positive normal: 2^-14
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x0400)), asBits(fromBits(0x38800000)));

    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x3C00)), asBits(1.0f));
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0xC000)), asBits(-2.0f));

    /// Largest finite normal: 65504
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x7BFF)), asBits(65504.0f));
}

TEST(FloatUtils, Infinity)
{
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0x7C00)), 0x7F800000u);
    EXPECT_BITS_EQ(asBits(convertFloat16ToFloat32(0xFC00)), 0xFF800000u);
}

TEST(FloatUtils, NaN)
{
    /// NaN bit patterns are not contractual; only its classification matters.
    EXPECT_TRUE(std::isnan(convertFloat16ToFloat32(0x7E00)));
    EXPECT_TRUE(std::isnan(convertFloat16ToFloat32(0xFE00)));
}
