#include <gtest/gtest.h>

#include <Access/Common/QuotaValueFromText.h>

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
    /// A hexadecimal float, whose exponent counts bits.
    EXPECT_EQ(scale("0x20000000000001p0", 1), exact(9007199254740993));
    EXPECT_EQ(scale("0x1.8p1", 1), exact(3));
    EXPECT_EQ(scale("0x1.8p0", 1).integral, false);
    EXPECT_EQ(scale("0x1p64", 1), does_not_fit);
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
}
