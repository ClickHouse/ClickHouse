#pragma once

#include <Access/Common/QuotaDefs.h>

#include <optional>
#include <string_view>


namespace DB
{

/// A numeric value as written in a query or in the configuration: the digits before and after the
/// point, and the exponent. The text is needed because the `Float64` obtained by parsing it has
/// already been rounded to the nearest double, which changes an integer above 2^53
/// (9007199254740993 becomes 9007199254740992), erases the fractional part of such a value
/// (9007199254740992.5), and can push a value at the top of the range out of it
/// (18446744073.709551615 scaled by 10^9 is exactly `UInt64` max, but rounds to 2^64).
struct NumericLiteralParts
{
    bool is_hex = false;
    std::string_view integer_part;
    std::string_view fractional_part;
    Int64 exponent = 0;
};

/// Splits a numeric value into its parts. Handles a decimal form with an optional exponent
/// (1.5, 1.5e1) and a hexadecimal float (0x1.8p1); returns nothing for any other text (e.g. inf, nan).
std::optional<NumericLiteralParts> splitNumericLiteral(std::string_view text);

/// Whether the value multiplied by `multiplier` is an integer.
/// `multiplier` is the output denominator of a quota type and must be a power of ten.
bool isIntegralScaledNumericLiteral(const NumericLiteralParts & parts, UInt64 multiplier);

/// The value multiplied by `multiplier` and truncated toward zero, computed exactly from the text,
/// or nothing if it does not fit into `QuotaValue` or has too many digits to be computed exactly.
/// `multiplier` is the output denominator of a quota type and must be a power of ten.
std::optional<QuotaValue> exactScaledValueOfNumericLiteral(const NumericLiteralParts & parts, UInt64 multiplier);

}
