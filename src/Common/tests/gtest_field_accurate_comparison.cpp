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

    /// Comparison of 2 non-equal negative values
    Field decimal_neg_33_30 = DecimalField<Decimal64>(-3330, 2);
    Field float_neg_44_40 = Float64(-44.40);
    EXPECT_FALSE(accurateLess(decimal_neg_33_30, float_neg_44_40));
    EXPECT_TRUE(accurateLess(float_neg_44_40, decimal_neg_33_30));
    EXPECT_FALSE(accurateEquals(decimal_neg_33_30, float_neg_44_40));
    EXPECT_FALSE(accurateLessOrEqual(decimal_neg_33_30, float_neg_44_40));
}

/// Test Decimal vs NaN comparisons
/// NaN should be treated as greater than all values (nan_direction_hint = 1).
/// This is critical for range analysis consistency.
TEST(FieldAccurateComparisonTest, DecimalVsNaN)
{
    Field decimal_val = DecimalField<Decimal64>(4440, 2);
    Field float_nan = Float64(std::numeric_limits<double>::quiet_NaN());

    /// equals: NaN is not equal to anything
    EXPECT_FALSE(accurateEquals(decimal_val, float_nan));
    EXPECT_FALSE(accurateEquals(float_nan, decimal_val));
    EXPECT_FALSE(accurateEquals(float_nan, float_nan));   /// same-type NaN: accurate::equalsOp treats NaN == NaN as false

    /// less: NaN is greater than all values
    EXPECT_TRUE(accurateLess(decimal_val, float_nan));   /// decimal < NaN
    EXPECT_FALSE(accurateLess(float_nan, decimal_val));  /// NaN is not less than decimal

    /// lessOrEqual: derived from less as !less(r, l)
    EXPECT_TRUE(accurateLessOrEqual(decimal_val, float_nan));   /// decimal <= NaN
    EXPECT_FALSE(accurateLessOrEqual(float_nan, decimal_val));  /// NaN <= decimal is false

    /// Negative decimal vs NaN
    Field decimal_neg = DecimalField<Decimal64>(-4440, 2);
    EXPECT_TRUE(accurateLess(decimal_neg, float_nan));
    EXPECT_FALSE(accurateLess(float_nan, decimal_neg));

    /// Zero decimal vs NaN
    Field decimal_zero = DecimalField<Decimal64>(0, 2);
    EXPECT_TRUE(accurateLess(decimal_zero, float_nan));
    EXPECT_FALSE(accurateLess(float_nan, decimal_zero));
    EXPECT_FALSE(accurateEquals(decimal_zero, float_nan));
}
