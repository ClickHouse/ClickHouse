#include <gtest/gtest.h>

#include <IO/ReadHelpers.h>

using namespace DB;

TEST(ParseWithSizeSuffix, Unchecked)
{
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("1")), 1ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("1K")), 1000ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("1Ki")), 1024ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("2M")), 2000000ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("3Gi")), 3ULL << 30);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("1T")), 1000000000000ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64>("1Ti")), 1ULL << 40);
}

TEST(ParseWithSizeSuffix, CheckOverflow)
{
    constexpr auto checked = ReadIntTextCheckOverflow::CHECK_OVERFLOW;

    EXPECT_EQ((parseWithSizeSuffix<UInt64, checked>("12K")), 12000ULL);
    EXPECT_EQ((parseWithSizeSuffix<UInt64, checked>("12Ti")), 12ULL << 40);
    EXPECT_EQ((parseWithSizeSuffix<UInt64, checked>("18446744073709551615")), 18446744073709551615ULL);

    /// Overflow in the number itself.
    EXPECT_THROW((parseWithSizeSuffix<UInt64, checked>("18446744073709551616")), Exception);
    /// Overflow in the multiplication by the suffix.
    EXPECT_THROW((parseWithSizeSuffix<UInt64, checked>("18446744073709552T")), Exception);
    EXPECT_THROW((parseWithSizeSuffix<UInt64, checked>("18446744073709551616K")), Exception);
}

TEST(ParseWithSizeSuffix, CheckOverflowNarrowType)
{
    constexpr auto checked = ReadIntTextCheckOverflow::CHECK_OVERFLOW;

    /// The suffix multiplier itself is not representable in the narrow type.
    EXPECT_THROW((parseWithSizeSuffix<UInt32, checked>("1T")), Exception);
    EXPECT_THROW((parseWithSizeSuffix<UInt32, checked>("1Ti")), Exception);
    EXPECT_THROW((parseWithSizeSuffix<Int32, checked>("1T")), Exception);
    EXPECT_THROW((parseWithSizeSuffix<UInt16, checked>("1M")), Exception);
    EXPECT_THROW((parseWithSizeSuffix<UInt8, checked>("1K")), Exception);

    /// Zero times anything is zero, no overflow.
    EXPECT_EQ((parseWithSizeSuffix<UInt32, checked>("0T")), 0U);
    EXPECT_EQ((parseWithSizeSuffix<UInt8, checked>("0Ki")), 0U);

    /// Values that fit after multiplication.
    EXPECT_EQ((parseWithSizeSuffix<UInt32, checked>("1G")), 1000000000U);
    EXPECT_EQ((parseWithSizeSuffix<UInt32, checked>("3Gi")), 3U << 30);
    EXPECT_THROW((parseWithSizeSuffix<UInt32, checked>("4Gi")), Exception);
    EXPECT_EQ((parseWithSizeSuffix<Int32, checked>("2G")), 2000000000);
    EXPECT_THROW((parseWithSizeSuffix<Int32, checked>("3G")), Exception);
}
