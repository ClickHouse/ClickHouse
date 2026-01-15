#include <gtest/gtest.h>
#include <Core/Field.h>
#include <Common/FieldAccurateComparison.h>

using namespace DB;

/// Test Decimal vs Float64 comparison
TEST(FieldAccurateComparisonTest, DecimalVsFloat64)
{
    /// 44.40 as Decimal64 vs Float64
    Field decimal_44_40 = DecimalField<Decimal64>(4440, 2);
    Field float_44_40 = Float64(44.40);
    Field float_44_39 = Float64(44.39);
    Field float_44_41 = Float64(44.41);

    EXPECT_TRUE(accurateEquals(decimal_44_40, float_44_40));
    EXPECT_TRUE(accurateEquals(float_44_40, decimal_44_40));
    EXPECT_FALSE(accurateEquals(decimal_44_40, float_44_39));

    EXPECT_FALSE(accurateLess(decimal_44_40, float_44_40));
    EXPECT_TRUE(accurateLess(decimal_44_40, float_44_41));
    EXPECT_TRUE(accurateLess(float_44_39, decimal_44_40));

    EXPECT_TRUE(accurateLessOrEqual(decimal_44_40, float_44_40));
    EXPECT_TRUE(accurateLessOrEqual(decimal_44_40, float_44_41));

    /// Negative values
    Field decimal_neg = DecimalField<Decimal64>(-4440, 2);
    Field float_neg = Float64(-44.40);
    EXPECT_TRUE(accurateEquals(decimal_neg, float_neg));
    EXPECT_TRUE(accurateLess(decimal_neg, decimal_44_40));
}
