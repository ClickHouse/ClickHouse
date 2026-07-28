#include <Access/Common/QuotaValueFromText.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/arithmeticOverflow.h>
#include <base/hex.h>

#include <bit>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{
    /// The number of decimal zeros of a power of ten, e.g. 3 for 1000.
    size_t decimalZerosOfPowerOfTen(UInt64 multiplier)
    {
        size_t zeros = 0;
        for (UInt64 rest = multiplier; rest != 1; rest /= 10, ++zeros)
        {
            if (rest == 0 || rest % 10 != 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a power of ten, got {}", multiplier);
        }
        return zeros;
    }
}

std::optional<NumericLiteralParts> splitNumericLiteral(std::string_view text)
{
    if (!text.empty() && (text.front() == '+' || text.front() == '-'))
        text.remove_prefix(1);

    NumericLiteralParts parts;
    parts.is_hex = text.starts_with("0x") || text.starts_with("0X");
    if (parts.is_hex)
        text.remove_prefix(2);

    auto is_digit = [&parts](char c) { return parts.is_hex ? isHexDigit(c) : isNumericASCII(c); };

    size_t i = 0;
    while (i < text.size() && is_digit(text[i]))
        ++i;
    parts.integer_part = text.substr(0, i);

    if (i < text.size() && text[i] == '.')
    {
        size_t fraction_begin = ++i;
        while (i < text.size() && is_digit(text[i]))
            ++i;
        parts.fractional_part = text.substr(fraction_begin, i - fraction_begin);
    }

    /// The exponent shifts the point by decimal digits for a decimal value ('e')
    /// and by bits for a hexadecimal one ('p'); its own digits are decimal in both cases.
    char exponent_char = parts.is_hex ? 'p' : 'e';
    if (i < text.size() && (text[i] | 0x20) == exponent_char)
    {
        ++i;
        bool exponent_negative = false;
        if (i < text.size() && (text[i] == '+' || text[i] == '-'))
        {
            exponent_negative = (text[i] == '-');
            ++i;
        }
        while (i < text.size() && isNumericASCII(text[i]))
        {
            /// Clamp: the point cannot usefully shift by more than the mantissa length anyway.
            parts.exponent = std::min<Int64>(parts.exponent * 10 + (text[i] - '0'), 1000000000);
            ++i;
        }
        if (exponent_negative)
            parts.exponent = -parts.exponent;
    }

    if (i != text.size() || (parts.integer_part.empty() && parts.fractional_part.empty()))
        return {}; /// Not a numeric form we understand: leave it to the checks on the value.

    return parts;
}

bool isIntegralScaledNumericLiteral(const NumericLiteralParts & parts, UInt64 multiplier)
{
    /// Multiplying a decimal value by 10^n shifts the point by n digits, and a hexadecimal one,
    /// whose exponent counts bits, by n bits: the remaining factor 5^n of the multiplier is odd
    /// and so cannot make a value with a fractional part integral.
    const Int64 exponent = parts.exponent + static_cast<Int64>(decimalZerosOfPowerOfTen(multiplier));

    /// Find the deepest nonzero digit relative to the point: fractional digits have depths
    /// 1, 2, ..., integer digits have depths 0, -1, ... counting leftwards from the lowest one.
    Int64 deepest_position = 0;
    char deepest_digit = 0;
    for (size_t k = 0; k < parts.fractional_part.size(); ++k)
    {
        if (parts.fractional_part[k] != '0')
        {
            deepest_position = static_cast<Int64>(k) + 1;
            deepest_digit = parts.fractional_part[k];
        }
    }
    if (deepest_digit == 0)
    {
        for (size_t k = 0; k < parts.integer_part.size(); ++k)
        {
            if (parts.integer_part[k] != '0')
            {
                deepest_position = static_cast<Int64>(k) + 1 - static_cast<Int64>(parts.integer_part.size());
                deepest_digit = parts.integer_part[k];
            }
        }
    }
    if (deepest_digit == 0)
        return true; /// All digits are zero: the value is 0.

    if (parts.is_hex)
    {
        /// A hexadecimal digit at depth d occupies the bits at depths 4d-3 .. 4d after the
        /// binary point; the deepest set bit of the digit decides integrality.
        Int64 deepest_bit = 4 * deepest_position - std::countr_zero(static_cast<unsigned>(unhex(deepest_digit)));
        return deepest_bit <= exponent;
    }
    return deepest_position <= exponent;
}

std::optional<QuotaValue> exactScaledValueOfNumericLiteral(const NumericLiteralParts & parts, UInt64 multiplier)
{
    const size_t multiplier_zeros = decimalZerosOfPowerOfTen(multiplier);
    const QuotaValue base = parts.is_hex ? 16 : 10;

    /// Trailing zeros of the fractional part do not change the value, while multiplying them in
    /// could overflow QuotaValue for a value that fits (e.g. 18446744073709551615.0).
    std::string_view fractional_part = parts.fractional_part;
    while (fractional_part.ends_with('0'))
        fractional_part.remove_suffix(1);

    /// Decimal digits below the scale are truncated away in the end; dropping them before the
    /// accumulation keeps a value that fits exact instead of overflowing the accumulation
    /// (e.g. 18446744073.7095516155 scaled by 10^9 is QuotaValue max and a half).
    if (!parts.is_hex)
    {
        Int64 surviving_digits = parts.exponent + static_cast<Int64>(multiplier_zeros);
        if (surviving_digits >= 0 && static_cast<Int64>(fractional_part.size()) > surviving_digits)
            fractional_part = fractional_part.substr(0, static_cast<size_t>(surviving_digits));
    }

    QuotaValue value = 0;
    for (std::string_view digits : {parts.integer_part, fractional_part})
    {
        for (char c : digits)
        {
            QuotaValue digit = parts.is_hex ? unhex(c) : static_cast<QuotaValue>(c - '0');
            if (common::mulOverflow(value, base, value) || common::addOverflow(value, digit, value))
                return {};
        }
    }

    if (value == 0)
        return value;

    /// The multiplier 10^n shifts a decimal value by n digits and a hexadecimal one, whose exponent
    /// counts bits, by n bits; the remaining factor 5^n of a hexadecimal multiplication is applied
    /// here, before the shift, so that the truncation below is done on the multiplied value.
    if (parts.is_hex)
    {
        for (size_t k = 0; k < multiplier_zeros; ++k)
        {
            if (common::mulOverflow(value, static_cast<QuotaValue>(5), value))
                return {};
        }
    }

    /// The digits of a hexadecimal value are four bits each, while its exponent counts bits.
    Int64 shift = parts.exponent + static_cast<Int64>(multiplier_zeros)
        - static_cast<Int64>(fractional_part.size()) * (parts.is_hex ? 4 : 1);
    const QuotaValue unit = parts.is_hex ? 2 : 10;
    for (; shift < 0 && value != 0; ++shift)
        value /= unit;
    for (; shift > 0; --shift)
    {
        if (common::mulOverflow(value, unit, value))
            return {};
    }

    return value;
}

}
