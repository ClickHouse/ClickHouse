#include <Common/LloydMaxQuantizer.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numbers>

namespace DB
{
namespace
{

uint32_t floatBits(Float32 value)
{
    return std::bit_cast<uint32_t>(value);
}

uint32_t ulpDistance(Float32 lhs, Float32 rhs)
{
    const uint32_t lhs_bits = floatBits(lhs);
    const uint32_t rhs_bits = floatBits(rhs);
    return std::max(lhs_bits, rhs_bits) - std::min(lhs_bits, rhs_bits);
}

long double normalPDF(long double value)
{
    if (std::isinf(value))
        return 0;

    static const long double inverse_sqrt_two_pi = 1 / std::sqrt(2 * std::numbers::pi_v<long double>);
    return std::exp(-value * value / 2) * inverse_sqrt_two_pi;
}

long double normalCDF(long double value)
{
    if (value == -std::numeric_limits<long double>::infinity())
        return 0;
    if (value == std::numeric_limits<long double>::infinity())
        return 1;

    return std::erfc(-value / std::numbers::sqrt2_v<long double>) / 2;
}

Float32 conditionalMean(size_t precision, size_t positive_prefix)
{
    const size_t block_size = size_t{1} << (8 - precision);
    const size_t first_index = (positive_prefix + (size_t{1} << (precision - 1))) * block_size;
    const size_t last_index = first_index + block_size - 1;
    const long double lower = first_index == 0 ? -std::numeric_limits<long double>::infinity()
                                               : static_cast<long double>(LloydMax::BOUNDARIES[first_index - 1]);
    const long double upper
        = last_index == 255 ? std::numeric_limits<long double>::infinity() : static_cast<long double>(LloydMax::BOUNDARIES[last_index]);
    return static_cast<Float32>((normalPDF(lower) - normalPDF(upper)) / (normalCDF(upper) - normalCDF(lower)));
}

TEST(LloydMaxQuantizer, PrefixCentroidsMatchGaussianConditionalMeans)
{
    size_t centroid_index = 0;
    for (size_t precision = 1; precision < 8; ++precision)
    {
        const size_t positive_prefixes = size_t{1} << (precision - 1);
        for (size_t prefix = 0; prefix < positive_prefixes; ++prefix)
        {
            const Float32 expected = conditionalMean(precision, prefix);
            const Float32 actual = LloydMax::POSITIVE_PREFIX_CENTROIDS[centroid_index];
            EXPECT_LE(ulpDistance(actual, expected), 1u) << "precision=" << precision << " prefix=" << prefix;
            ++centroid_index;
        }
    }
    EXPECT_EQ(centroid_index, std::size(LloydMax::POSITIVE_PREFIX_CENTROIDS));
}

TEST(LloydMaxQuantizer, TransposedDequantLUTPreservesPrefixContract)
{
    const auto & lut = LloydMax::transposedDequantLUT();

    for (size_t precision = 1; precision < 8; ++precision)
    {
        const size_t block_size = size_t{1} << (8 - precision);
        for (size_t raw = 0; raw < 256; ++raw)
        {
            const Float32 actual = lut[precision][raw];
            const size_t block_start = raw & ~(block_size - 1);
            EXPECT_TRUE(std::isfinite(actual)) << "precision=" << precision << " raw=" << raw;
            EXPECT_EQ(floatBits(actual), floatBits(lut[precision][block_start])) << "precision=" << precision << " raw=" << raw;
            EXPECT_EQ(floatBits(actual), floatBits(-lut[precision][0xFFu - raw])) << "precision=" << precision << " raw=" << raw;
        }

        for (size_t index = 1; index < 256; ++index)
        {
            const Float32 previous = lut[precision][(index - 1) ^ 0x80u];
            const Float32 current = lut[precision][index ^ 0x80u];
            EXPECT_LE(previous, current) << "precision=" << precision << " index=" << index;
        }
    }

    for (size_t raw = 0; raw < 256; ++raw)
    {
        const auto index = static_cast<uint8_t>(raw ^ 0x80u);
        const Float32 expected = static_cast<Float32>(BFloat16(LloydMax::LEVELS[index]));
        EXPECT_EQ(floatBits(lut[8][raw]), floatBits(expected)) << "raw=" << raw;
    }
}

}
}
