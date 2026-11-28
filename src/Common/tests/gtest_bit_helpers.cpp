#include <Common/BitHelpers.h>

#include <cstddef>
#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

TEST(BitHelpers, RoundUpToPowerOfTwoOrZeroDocumentedEdgeCases)
{
    /// "For zero argument, result is zero."
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(0), 0uz);

    EXPECT_EQ(roundUpToPowerOfTwoOrZero(1), 1uz);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(2), 2uz);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(3), 4uz);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(4), 4uz);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(5), 8uz);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(1000), 1024uz);

    /// "For arguments with most significand bit set, result is n." Rounding those up is not
    /// representable, so the input is returned unchanged rather than zero.
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(1uz << 63), 1uz << 63);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero((1uz << 63) + 1), (1uz << 63) + 1);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(std::numeric_limits<size_t>::max()),
              std::numeric_limits<size_t>::max());
}

TEST(BitHelpers, RoundUpToPowerOfTwoOrZeroAboveThirtyTwoBits)
{
    /// The function is applied to allocation sizes (`PODArray`, `GraceHashJoin`, the cache
    /// dictionaries), so arguments above 2^32 are ordinary. They are also the arguments that
    /// tell a 64-bit count of leading zeros apart from a 32-bit one: with the latter, the
    /// argument would be truncated and every expectation below would come out as some small
    /// power of two.
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(1uz << 32), 1uz << 32);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero((1uz << 32) + 1), 1uz << 33);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero((1uz << 32) - 1), 1uz << 32);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero(3uz << 40), 1uz << 42);
    EXPECT_EQ(roundUpToPowerOfTwoOrZero((1uz << 62) + 1), 1uz << 63);

    /// Every power of two in the range is its own answer, and one more than it is the next one.
    for (size_t bit = 32; bit < 63; ++bit)
    {
        const size_t power = 1uz << bit;
        EXPECT_EQ(roundUpToPowerOfTwoOrZero(power), power) << "at bit " << bit;
        EXPECT_EQ(roundUpToPowerOfTwoOrZero(power + 1), power << 1) << "at bit " << bit;
    }
}
