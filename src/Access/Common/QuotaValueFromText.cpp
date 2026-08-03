#include <Access/Common/QuotaValueFromText.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/hex.h>

#include <boost/multiprecision/cpp_int.hpp>

#include <algorithm>
#include <bit>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


size_t decimalZerosOfPowerOfTen(UInt64 power_of_ten)
{
    size_t zeros = 0;
    for (UInt64 rest = power_of_ten; rest != 1; rest /= 10, ++zeros)
    {
        if (rest == 0 || rest % 10 != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a power of ten, got {}", power_of_ten);
    }
    return zeros;
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

    /// Leading zeros of the integer part and trailing zeros of the fractional part do not change
    /// the value, and dropping them keeps the accumulation below shorter.
    std::string_view integer_part = parts.integer_part;
    std::string_view fractional_part = parts.fractional_part;
    while (integer_part.starts_with('0'))
        integer_part.remove_prefix(1);
    while (fractional_part.ends_with('0'))
        fractional_part.remove_suffix(1);

    const size_t digits_count = integer_part.size() + fractional_part.size();

    /// The accumulation below is quadratic in the number of digits, so its length is bounded.
    /// The bound is far above the longest form of a `Float64`: about 770 digits for a decimal one
    /// and about 280 for a hexadecimal one, so no literal that a user can mean is left out.
    static constexpr size_t max_digits_count = 4096;
    if (digits_count > max_digits_count)
        return {};

    /// The digits of a hexadecimal value are four bits each, while its exponent counts bits.
    /// The scaled value is the mantissa digits shifted by this many digits (bits for a hexadecimal one).
    const Int64 shift = parts.exponent + static_cast<Int64>(multiplier_zeros)
        - static_cast<Int64>(fractional_part.size()) * (parts.is_hex ? 4 : 1);

    /// The mantissa is accumulated in an arbitrary precision integer, because a mantissa that is
    /// wider than any fixed type can still give a value in range once a negative exponent truncates
    /// its lowest digits away, and those digits cannot be dropped before the multiplication by the
    /// remaining factor 5^n of the multiplier below, which carries them upwards
    /// (0x100000000000000020000000000000001p-94 seconds is exactly 17179869184.000000001, so its
    /// 129 bits of mantissa give a value of 65 bits after the scaling by 10^9).
    boost::multiprecision::cpp_int value = 0;
    const unsigned base = parts.is_hex ? 16 : 10;
    for (std::string_view digits : {integer_part, fractional_part})
    {
        for (char c : digits)
            value = value * base + static_cast<unsigned>(parts.is_hex ? unhex(c) : c - '0');
    }

    /// The multiplier 10^n shifts a decimal value by n digits and a hexadecimal one, whose exponent
    /// counts bits, by n bits; the remaining factor 5^n of a hexadecimal multiplication is applied
    /// here, before the shift, so that the truncation below is done on the multiplied value.
    if (parts.is_hex)
    {
        for (size_t k = 0; k < multiplier_zeros; ++k)
            value *= 5;
    }

    if (shift < 0 && value != 0)
    {
        const UInt64 amount = static_cast<UInt64>(-shift);
        if (parts.is_hex)
        {
            /// A shift that covers the whole value truncates it to zero; the exponent can be large
            /// enough to make the shift itself pointless to perform.
            const UInt64 bits = static_cast<UInt64>(boost::multiprecision::msb(value)) + 1;
            if (amount >= bits)
                value = 0;
            else
                value >>= amount;
        }
        else if (amount >= digits_count)
        {
            /// The value is below 10 to the power of the number of its digits.
            value = 0;
        }
        else
        {
            for (UInt64 k = 0; k < amount; ++k)
                value /= 10;
        }
    }
    else if (shift > 0 && value != 0)
    {
        /// The result has to fit into 64 bits, so a larger shift cannot give a value in range.
        if (shift > 64)
            return {};
        for (Int64 k = 0; k < shift; ++k)
            value *= parts.is_hex ? 2 : 10;
    }

    if (value > std::numeric_limits<QuotaValue>::max())
        return {};
    return static_cast<QuotaValue>(value);
}

}
