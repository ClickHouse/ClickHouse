#include <gtest/gtest.h>

#include <Access/Common/QuotaValueFromText.h>

#include <limits>

using namespace DB;

namespace
{
    /// The outcome of taking the exact scaled value of a numeric text, as the quota parsers see it.
    struct Outcome
    {
        bool recognized = false;
        bool integral = false;
        bool fits = false;
        QuotaValue value = 0;

        bool operator==(const Outcome &) const = default;
    };

    Outcome scale(std::string_view text, UInt64 multiplier)
    {
        auto parts = splitNumericLiteral(text);
        if (!parts)
            return {};
        Outcome outcome{.recognized = true, .integral = isIntegralScaledNumericLiteral(*parts, multiplier)};
        if (auto value = exactScaledValueOfNumericLiteral(*parts, multiplier))
        {
            outcome.fits = true;
            outcome.value = *value;
        }
        return outcome;
    }

    Outcome exact(QuotaValue value, bool integral = true)
    {
        return {.recognized = true, .integral = integral, .fits = true, .value = value};
    }

    constexpr Outcome unrecognized{};
    constexpr Outcome does_not_fit{.recognized = true, .integral = true, .fits = false, .value = 0};

    constexpr UInt64 nanoseconds = 1000000000; /// The output denominator of `execution_time`.
}

/// A quota type without an output denominator: the value comes from the text as written.
TEST(QuotaValueFromText, Unscaled)
{
    EXPECT_EQ(scale("0", 1), exact(0));
    EXPECT_EQ(scale("100", 1), exact(100));
    EXPECT_EQ(scale("1.5e1", 1), exact(15));
    EXPECT_EQ(scale("1.5", 1).integral, false);
    EXPECT_EQ(scale("15e-1", 1).integral, false);
    /// Above 2^53 the value must not go through a double, which would round it.
    EXPECT_EQ(scale("9007199254740993.0", 1), exact(9007199254740993));
    EXPECT_EQ(scale("18446744073709551615.0", 1), exact(18446744073709551615ULL));
    EXPECT_EQ(scale("18446744073709551616", 1), does_not_fit);
    EXPECT_EQ(scale("1e20", 1), does_not_fit);
    /// A mantissa that fits into the range only after a negative exponent shifts its lowest digits
    /// away: those digits are discarded before the value is computed, instead of overflowing it.
    EXPECT_EQ(scale("184467440737095516150e-1", 1), exact(18446744073709551615ULL));
    EXPECT_EQ(scale("1844674407370955161500e-2", 1), exact(18446744073709551615ULL));
    EXPECT_EQ(scale("184467440737095516159e-1", 1), exact(18446744073709551615ULL, /* integral= */ false));
    EXPECT_EQ(scale("184467440737095516160e-1", 1), does_not_fit);
    /// A hexadecimal float, whose exponent counts bits.
    EXPECT_EQ(scale("0x20000000000001p0", 1), exact(9007199254740993));
    EXPECT_EQ(scale("0x1.8p1", 1), exact(3));
    EXPECT_EQ(scale("0x1.8p0", 1).integral, false);
    EXPECT_EQ(scale("0x1p64", 1), does_not_fit);
    /// A wide hexadecimal mantissa that fits into the range only after a negative exponent shifts
    /// its lowest digits away: those digits are zero, so they are discarded before the value is
    /// computed instead of overflowing it (0x200000000000010000000000000000000p-76 is 2^53 + 1).
    EXPECT_EQ(scale("0x200000000000010000000000000000000p-76", 1), exact(9007199254740993));
    EXPECT_EQ(scale("0x2000000000000100000000000000000000p-80", 1), exact(9007199254740993));
    EXPECT_EQ(scale("0x200000000000010000000000000000000p-77", 1), exact(4503599627370496, /* integral= */ false));
    /// Not a numeric form the analysis understands.
    EXPECT_EQ(scale("inf", 1), unrecognized);
    EXPECT_EQ(scale("nan", 1), unrecognized);
    EXPECT_EQ(scale("12K", 1), unrecognized);
    EXPECT_EQ(scale("", 1), unrecognized);
}

/// A quota type with an output denominator: the value is multiplied by it exactly, so that a value
/// at the top of the range is neither rejected nor altered by the rounding of a double.
TEST(QuotaValueFromText, Scaled)
{
    EXPECT_EQ(scale("0", nanoseconds), exact(0));
    EXPECT_EQ(scale("1.5", nanoseconds), exact(1500000000));
    EXPECT_EQ(scale("2.5", nanoseconds), exact(2500000000));
    EXPECT_EQ(scale("18446744073", nanoseconds), exact(18446744073000000000ULL));
    EXPECT_EQ(scale("18446744073.709551615", nanoseconds), exact(18446744073709551615ULL));
    EXPECT_EQ(scale("18446744073.709551615e0", nanoseconds), exact(18446744073709551615ULL));
    EXPECT_EQ(scale("18446744073.709551616", nanoseconds), does_not_fit);
    EXPECT_EQ(scale("1e19", nanoseconds), does_not_fit);
    /// A value below a whole nanosecond is truncated, as the cast of a scaled double truncates it.
    EXPECT_EQ(scale("1e-9", nanoseconds), exact(1));
    EXPECT_EQ(scale("1e-10", nanoseconds), exact(0, /* integral= */ false));
    EXPECT_EQ(scale("1844674407.3709551615", nanoseconds), exact(1844674407370955161, /* integral= */ false));
    /// Digits below the scale are truncated even when the value has more of them than fit.
    EXPECT_EQ(scale("18446744073.7095516155", nanoseconds), exact(18446744073709551615ULL, /* integral= */ false));
    /// A hexadecimal float is scaled exactly too: the denominator contributes bits and a factor of 5^n.
    EXPECT_EQ(scale("0x1.8p1", nanoseconds), exact(3000000000));
    EXPECT_EQ(scale("0x1.8p0", nanoseconds), exact(1500000000));
    EXPECT_EQ(scale("0x3p-31", nanoseconds), exact(1, /* integral= */ false));
    EXPECT_EQ(scale("0x1p64", nanoseconds), does_not_fit);
    /// A wide hexadecimal mantissa is scaled exactly as well (0x30000000000000000p-64 is 3).
    EXPECT_EQ(scale("0x30000000000000000p-64", nanoseconds), exact(3000000000));
    /// The exponent form of the value at the top of the range: its mantissa does not fit on its own.
    EXPECT_EQ(scale("184467440737095516150e-10", nanoseconds), exact(18446744073709551615ULL));
    /// A hexadecimal mantissa wider than any fixed accumulator: it fits into the range only after the
    /// exponent shifts its lowest bits away, and those bits cannot be dropped before the value is
    /// accumulated, because the multiplication by the factor 5^n of the denominator carries them
    /// upwards (0x100000000000000020000000000000001p-94 seconds is 17179869184.000000001).
    EXPECT_EQ(
        scale("0x100000000000000020000000000000001p-94", nanoseconds),
        exact(17179869184000000001ULL, /* integral= */ false));
    EXPECT_EQ(
        scale("0x1000000000000000200000000000000010p-98", nanoseconds),
        exact(17179869184000000001ULL, /* integral= */ false));
}

/// A scaled value must be shown back exactly, so that the output of `SHOW CREATE QUOTA` can be
/// replayed: it used to be rendered through a double, which rounds the top of the range out of it.
TEST(QuotaValueFromText, ScaledRoundTrip)
{
    const auto & info = QuotaTypeInfo::get(QuotaType::EXECUTION_TIME);

    EXPECT_EQ(info.valueToString(std::numeric_limits<QuotaValue>::max()), "18446744073.709551615");
    EXPECT_EQ(info.valueToString(18446744073000000000ULL), "18446744073");
    EXPECT_EQ(info.valueToString(1500000000), "1.5");
    EXPECT_EQ(info.valueToString(1), "0.000000001");
    EXPECT_EQ(info.valueToString(0), "0");

    for (QuotaValue value : {QuotaValue(0),
                             QuotaValue(1),
                             QuotaValue(1500000000),
                             QuotaValue(18446744073000000000ULL),
                             QuotaValue(18446744073709551614ULL),
                             std::numeric_limits<QuotaValue>::max()})
        EXPECT_EQ(info.stringToValue(info.valueToString(value)), value) << info.valueToString(value);
}
