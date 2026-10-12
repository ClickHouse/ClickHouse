#include <gtest/gtest.h>

#include <Storages/MergeTree/SmallFloat.h>

#include <climits>
#include <limits>

using namespace DB;

/// The constants the encoding derives from: toInt4(2^24 - 1) == 175, leaving
/// 255 - 175 == 80 byte values for exact (one-to-one) low-value encoding.
/// (Lucene uses INT_MAX as the cap, giving 231/24; the algorithm is the same.)
TEST(SmallFloat, Constants)
{
    EXPECT_EQ(SmallFloat::MAX_ENCODABLE_VALUE, (1u << 24) - 1);
    EXPECT_EQ(SmallFloat::MAX_INT4, 175u);
    EXPECT_EQ(SmallFloat::NUM_FREE_VALUES, 80u);
}

/// Known (input -> byte) pairs produced by the Lucene `intToByte4` algorithm with the
/// 2^24 - 1 saturation cap.
TEST(SmallFloat, KnownValues)
{
    EXPECT_EQ(SmallFloat::toInt4Byte(0), 0);
    EXPECT_EQ(SmallFloat::toInt4Byte(1), 1);
    EXPECT_EQ(SmallFloat::toInt4Byte(24), 24);
    EXPECT_EQ(SmallFloat::toInt4Byte(79), 79);
    EXPECT_EQ(SmallFloat::toInt4Byte(80), 80);
    EXPECT_EQ(SmallFloat::toInt4Byte(100), 98);
    EXPECT_EQ(SmallFloat::toInt4Byte(255), 122);
    EXPECT_EQ(SmallFloat::toInt4Byte(1000), 142);
    EXPECT_EQ(SmallFloat::toInt4Byte(SmallFloat::MAX_ENCODABLE_VALUE), 255);

    EXPECT_EQ(SmallFloat::fromInt4Byte(0), 0u);
    EXPECT_EQ(SmallFloat::fromInt4Byte(79), 79u);
    EXPECT_EQ(SmallFloat::fromInt4Byte(98), 100u);
    EXPECT_EQ(SmallFloat::fromInt4Byte(122), 240u);
    EXPECT_EQ(SmallFloat::fromInt4Byte(142), 976u);
    EXPECT_EQ(SmallFloat::fromInt4Byte(255), 15728720u);
}

/// Values below NUM_FREE_VALUES are encoded exactly and round-trip losslessly.
TEST(SmallFloat, ExactLowValues)
{
    for (UInt32 i = 0; i < SmallFloat::NUM_FREE_VALUES; ++i)
    {
        EXPECT_EQ(SmallFloat::toInt4Byte(i), static_cast<UInt8>(i));
        EXPECT_EQ(SmallFloat::fromInt4Byte(SmallFloat::toInt4Byte(i)), i);
    }
}

/// Decoding is a floor: the decoded value never exceeds the original (the encoding
/// truncates the low bits). Required for the length-norm denominator to stay sound.
/// Scanned across the whole UInt32 domain — above MAX_ENCODABLE_VALUE the byte
/// saturates to 255 and fromInt4Byte(255) == 15728720 stays below the (larger) input,
/// so the floor still holds.
TEST(SmallFloat, DecodeIsFloor)
{
    for (UInt64 i = 0; i <= std::numeric_limits<UInt32>::max(); i += 1 + i / 64)
    {
        const UInt32 x = static_cast<UInt32>(i);
        EXPECT_LE(SmallFloat::fromInt4Byte(SmallFloat::toInt4Byte(x)), x) << "x=" << x;
    }
}

/// Load-bearing: `toInt4Byte` is monotonic non-decreasing in its argument, so the
/// per-block `min_dl_byte` reduce (block-max input) is a plain unsigned byte compare.
/// Scans the FULL UInt32 domain, including the region above the saturation cap.
TEST(SmallFloat, MonotonicNonDecreasing)
{
    UInt8 prev = 0;
    for (UInt64 i = 0; i <= std::numeric_limits<UInt32>::max(); i += 1 + i / 128)
    {
        const UInt8 b = SmallFloat::toInt4Byte(static_cast<UInt32>(i));
        EXPECT_GE(b, prev) << "non-monotonic at i=" << i;
        prev = b;
    }
}

/// Above MAX_ENCODABLE_VALUE the byte saturates to 255 rather than wrapping.
TEST(SmallFloat, SaturatesAboveCap)
{
    EXPECT_EQ(SmallFloat::toInt4Byte(SmallFloat::MAX_ENCODABLE_VALUE), 255);
    EXPECT_EQ(SmallFloat::toInt4Byte(SmallFloat::MAX_ENCODABLE_VALUE + 1), 255);
    EXPECT_EQ(SmallFloat::toInt4Byte(static_cast<UInt32>(INT_MAX)), 255);
    EXPECT_EQ(SmallFloat::toInt4Byte(std::numeric_limits<UInt32>::max()), 255);
}
