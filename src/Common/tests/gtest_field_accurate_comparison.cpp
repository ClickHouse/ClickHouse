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

/// Test Decimal vs the Null-typed sentinels (NULL, -Inf, +Inf).
/// These exercise the `lt != rt` fast paths: a real value never equals a sentinel,
/// and ordering against -Inf/+Inf is decided purely by the sentinel.
TEST(FieldAccurateComparisonTest, DecimalVsNullAndInfinity)
{
    Field decimal_val = DecimalField<Decimal64>(4440, 2);
    Field pos_inf = POSITIVE_INFINITY;
    Field neg_inf = NEGATIVE_INFINITY;
    Field null_val = Null();

    /// equals: a real value never equals NULL / -Inf / +Inf (matches FieldVisitorAccurateEquals).
    EXPECT_FALSE(accurateEquals(decimal_val, pos_inf));
    EXPECT_FALSE(accurateEquals(pos_inf, decimal_val));
    EXPECT_FALSE(accurateEquals(decimal_val, neg_inf));
    EXPECT_FALSE(accurateEquals(neg_inf, decimal_val));
    EXPECT_FALSE(accurateEquals(decimal_val, null_val));
    EXPECT_FALSE(accurateEquals(null_val, decimal_val));

    /// less: everything is below +Inf and above -Inf.
    EXPECT_TRUE(accurateLess(decimal_val, pos_inf));
    EXPECT_FALSE(accurateLess(pos_inf, decimal_val));
    EXPECT_FALSE(accurateLess(decimal_val, neg_inf));
    EXPECT_TRUE(accurateLess(neg_inf, decimal_val));

    /// lessOrEqual mirrors less for the strict cases here.
    EXPECT_TRUE(accurateLessOrEqual(decimal_val, pos_inf));
    EXPECT_FALSE(accurateLessOrEqual(pos_inf, decimal_val));
    EXPECT_FALSE(accurateLessOrEqual(decimal_val, neg_inf));
    EXPECT_TRUE(accurateLessOrEqual(neg_inf, decimal_val));
}

/// Test same-Field-type Decimal comparisons, which take the same-type fast path.
/// Covers both the equal-scale native-integer shortcut and the different-scale
/// scale-aware fallback; results must match the scale-aware `DecimalField` operators.
TEST(FieldAccurateComparisonTest, DecimalSameTypeScales)
{
    /// Equal scale -> native-integer compare shortcut.
    Field a = DecimalField<Decimal64>(4440, 2);   /// 44.40
    Field b = DecimalField<Decimal64>(4441, 2);   /// 44.41
    Field c = DecimalField<Decimal64>(4440, 2);   /// 44.40

    EXPECT_TRUE(accurateEquals(a, c));
    EXPECT_FALSE(accurateEquals(a, b));
    EXPECT_TRUE(accurateLess(a, b));
    EXPECT_FALSE(accurateLess(b, a));
    EXPECT_FALSE(accurateLess(a, c));
    EXPECT_TRUE(accurateLessOrEqual(a, c));
    EXPECT_TRUE(accurateLessOrEqual(a, b));
    EXPECT_FALSE(accurateLessOrEqual(b, a));

    /// Negative, equal scale.
    Field g = DecimalField<Decimal64>(-4440, 2);  /// -44.40
    Field h = DecimalField<Decimal64>(-4441, 2);  /// -44.41
    EXPECT_TRUE(accurateLess(h, g));
    EXPECT_FALSE(accurateLess(g, h));

    /// Different scale -> scale-aware fallback. 44.40 (scale 2) == 44.400 (scale 3).
    Field d = DecimalField<Decimal64>(4440, 2);
    Field e = DecimalField<Decimal64>(44400, 3);
    Field f = DecimalField<Decimal64>(44401, 3);  /// 44.401
    EXPECT_TRUE(accurateEquals(d, e));
    EXPECT_FALSE(accurateLess(d, e));
    EXPECT_TRUE(accurateLessOrEqual(d, e));
    EXPECT_TRUE(accurateLess(d, f));
    EXPECT_FALSE(accurateEquals(d, f));

    /// Wider decimal type, equal scale, to cover the Decimal128 fast path.
    Field w1 = DecimalField<Decimal128>(Decimal128(100), 2);
    Field w2 = DecimalField<Decimal128>(Decimal128(101), 2);
    EXPECT_TRUE(accurateLess(w1, w2));
    EXPECT_FALSE(accurateEquals(w1, w2));
    EXPECT_TRUE(accurateLessOrEqual(w1, w1));
}

/// Pin the actual target carrier of the optimization: a `Field` built from
/// `DecimalField<DateTime64>` (which shares the `Field::Types::Decimal64` tag and
/// layout, so it is read back as `DecimalField<Decimal64>` by the fast paths).
/// Mirrors a `DateTime64(3)` range predicate: millisecond timestamps and half-open
/// bounds against the `+Inf` / `-Inf` sentinels.
TEST(FieldAccurateComparisonTest, DateTime64Carrier)
{
    /// 1780329306.294 and 1780329606.294 as DateTime64(3).
    Field dt1 = DecimalField<DateTime64>(DateTime64(1780329306294LL), 3);
    Field dt2 = DecimalField<DateTime64>(DateTime64(1780329606294LL), 3);
    Field dt1_again = DecimalField<DateTime64>(DateTime64(1780329306294LL), 3);

    /// Null / +Inf / -Inf shortcut against the DateTime64 carrier.
    EXPECT_FALSE(accurateEquals(dt1, POSITIVE_INFINITY));
    EXPECT_FALSE(accurateEquals(POSITIVE_INFINITY, dt1));
    EXPECT_FALSE(accurateEquals(dt1, NEGATIVE_INFINITY));
    EXPECT_FALSE(accurateEquals(dt1, Field(Null())));
    EXPECT_TRUE(accurateLess(dt1, POSITIVE_INFINITY));
    EXPECT_FALSE(accurateLess(POSITIVE_INFINITY, dt1));
    EXPECT_TRUE(accurateLess(NEGATIVE_INFINITY, dt1));
    EXPECT_FALSE(accurateLess(dt1, NEGATIVE_INFINITY));
    EXPECT_TRUE(accurateLessOrEqual(dt1, POSITIVE_INFINITY));
    EXPECT_FALSE(accurateLessOrEqual(POSITIVE_INFINITY, dt1));

    /// Same-scale fast path between two DateTime64 carriers.
    EXPECT_TRUE(accurateEquals(dt1, dt1_again));
    EXPECT_FALSE(accurateEquals(dt1, dt2));
    EXPECT_TRUE(accurateLess(dt1, dt2));
    EXPECT_FALSE(accurateLess(dt2, dt1));
    EXPECT_TRUE(accurateLessOrEqual(dt1, dt1_again));
    EXPECT_FALSE(accurateLessOrEqual(dt2, dt1));

    /// Cross-carrier: a DateTime64-built Field compared against a Decimal64-built
    /// Field of the same scale and value must compare equal -- this is exactly the
    /// reinterpretation the hot path relies on (granule value vs analyzed literal).
    Field dec_same = DecimalField<Decimal64>(1780329306294LL, 3);
    EXPECT_TRUE(accurateEquals(dt1, dec_same));
    EXPECT_FALSE(accurateLess(dt1, dec_same));
    EXPECT_TRUE(accurateLessOrEqual(dt1, dec_same));
}
