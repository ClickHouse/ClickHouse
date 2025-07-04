#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <Common/UTF8Helpers.h>
#include <Common/quoteString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/readDecimalText.h>
#include <IO/readIntTextInBase.h>


namespace DB
{

namespace
{
    /// Parses escape sequences in a string literal and replaces them with the characters which they mean.
    bool unescapeStringLiteral(std::string_view input, String & result, String & error_message, size_t & error_pos)
    {
        result.clear();
        result.reserve(input.length());

        for (size_t pos = 0; pos < input.length();)
        {
            size_t next_pos = input.find('\\', pos);
            if (next_pos == String::npos)
                next_pos = input.length();

            result.append(input.substr(pos, next_pos - pos));
            pos = next_pos;

            if (pos >= input.length())
                break;

            /// An escape sequences contains at least 2 characters.
            if (pos + 2 > input.length())
            {
                error_message = fmt::format("Invalid escape sequence {}: Expected at least 2 characters",
                                            quoteString(input.substr(pos)));
                error_pos = pos;
                return false;
            }

            chassert(input[pos] == '\\');
            char c = input[pos + 1];

            switch (c)
            {
                case 'a':  result.push_back(0x07); pos += 2; break;  /// \a  U+0007 alert or bell
                case 'b':  result.push_back(0x08); pos += 2; break;  /// \b  U+0008 backspace
                case 'f':  result.push_back(0x0C); pos += 2; break;  /// \f  U+000C form feed
                case 'n':  result.push_back(0x0A); pos += 2; break;  /// \n  U+000A line feed or newline
                case 'r':  result.push_back(0x0D); pos += 2; break;  /// \r  U+000D carriage return
                case 't':  result.push_back(0x09); pos += 2; break;  /// \t  U+0009 horizontal tab
                case 'v':  result.push_back(0x0B); pos += 2; break;  /// \v  U+000B vertical tab
                case '\\': result.push_back('\''); pos += 2; break;  /// \\  U+005C backslash
                case '\'': result.push_back('\''); pos += 2; break;  /// \'  U+0027 single quote
                case '"':  result.push_back('"');  pos += 2; break;  /// \"  U+0022 double quote
                case 'x': 
                {
                    /// \x followed by exactly two hexadecimal digits represents a single byte.
                    /// Example: \x51 is the 'Q' letter.
                    if (pos + 4 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Expected 4 characters",
                                                    quoteString(input.substr(pos)));
                        error_pos = pos;
                        return false;
                    }
                    char byte;
                    if (!tryParseIntInBase<16>(byte, input.substr(pos + 2, 2)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Cannot parse a hexadecimal number representing a single byte",
                                                    quoteString(input.substr(pos, 4)));
                        error_pos = pos;
                        return false;
                    }
                    result.push_back(byte);
                    pos += 4;
                    break;
                }
                case '0': [[fallthrough]];
                case '1': [[fallthrough]];
                case '2': [[fallthrough]];
                case '3': [[fallthrough]];
                case '4': [[fallthrough]];
                case '5': [[fallthrough]];
                case '6': [[fallthrough]];
                case '7':
                {
                    /// \nnn - three digits octal represents a single byte.
                    /// Example: \121 is the 'Q' letter.
                    if (pos + 4 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Expected 4 characters",
                                                    quoteString(input.substr(pos)));
                        error_pos = pos;
                        return false;
                    }
                    UInt16 byte;
                    if (!tryParseIntInBase<8>(byte, input.substr(pos + 1, 3)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Cannot parse an octal number representing a single byte",
                                                    quoteString(input.substr(pos, 4)));
                        error_pos = pos;
                        return false;
                    }
                    if (byte > 0xFF)
                    {
                        error_message = fmt::format("Invalid escape sequence {}: An octal representation \nnn must represent a single byte",
                                                    quoteString(input.substr(pos, 4)));
                        error_pos = pos;
                        return false;
                    }
                    result.push_back(static_cast<char>(byte));
                    pos += 4;
                    break;
                }
                case 'u':
                {
                    /// \u followed by exactly four hexadecimal digits represents a single Unicode code point.
                    /// Example: \u0051 is the 'Q' letter.
                    if (pos + 6 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Expected 6 characters",
                                                    quoteString(input.substr(pos)));
                        error_pos = pos;
                        return false;
                    }
                    UInt16 code_point;
                    if (!tryParseIntInBase<16>(code_point, input.substr(pos + 2, 4)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Cannot parse a hexadecimal number representing a Unicode code point",
                                                    quoteString(input.substr(pos, 6)));
                        error_pos = pos;
                        return false;
                    }
                    char bytes[3];  /// 3 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    result.append(bytes, num_bytes);
                    pos += 6;
                    break;
                }
                case 'U':
                {
                    /// \U followed by exactly eight hexadecimal digits represents a single Unicode code point.
                    /// Example: \U00000051 is the 'Q' letter.
                    if (pos + 10 > input.length())
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Expected 10 characters",
                                                    quoteString(input.substr(pos)));
                        error_pos = pos;
                        return false;
                    }
                    UInt32 code_point;
                    if (!tryParseIntInBase<16>(code_point, input.substr(pos + 2, 8)))
                    {
                        error_message = fmt::format("Invalid escape sequence {}: Cannot parse a hexadecimal number representing a Unicode code point",
                                                    quoteString(input.substr(pos, 10)));
                        error_pos = pos;
                        return false;
                    }
                    if (code_point > 0x10FFFF)  /// There should be no Unicode code point beyond 0x10FFFF.
                    {
                        error_message = fmt::format("Invalid escape sequence {}: A Unicode code point can't be greater than 0x10FFFF",
                                                    quoteString(input.substr(pos, 10)));
                        error_pos = pos;
                        return false;
                    }
                    char bytes[4];  /// 4 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    result.append(bytes, num_bytes);
                    pos += 10;
                    break;
                }
                default:
                {
                    error_message = fmt::format("Invalid escape sequence {}", quoteString(input.substr(pos)));
                    error_pos = pos;
                    return false;
                }
            }
        }
        return true;
    }
}

/// Converts a quoted string literal to its unquoted version.
bool PrometheusQueryParsingUtil::parseStringLiteral(std::string_view input, String & result,
                                                    String & error_message, size_t & error_pos)
{
    if (!input.starts_with('\'') && !input.starts_with('\"') && !input.starts_with('`'))
    {
        error_message = fmt::format("Cannot parse string literal {}: A string literal must open with a quote ', a double quote \" or a backtick `", input);
        error_pos = 0;
        return false;
    }

    char quote_char = input[0];

    if ((input.length() < 2) || !input.ends_with(quote_char))
    {
        std::string_view quote_char_name = (quote_char == '\'') ? "quote" : ((quote_char == '\"') ? "double quote" : "backtick");
        error_message = fmt::format("Cannot parse string literal {}: No closing {} {}", input, quote_char_name, quote_char);
        error_pos = input.length() - 1;
        return false;
    }

    /// A string literal enclosed in backticks: escape sequences are not parsed.
    if (quote_char == '`')
    {
        size_t closing_backtick = input.find('`', 1);
        if (closing_backtick != input.length() - 1)
        {
            error_message = fmt::format("Cannot parse string literal {}: A string literal in backticks can't contain other backticks", input);
            error_pos = closing_backtick;
            return false;
        }
        result = input.substr(1, input.length() - 2);
        return true;
    }

    /// A string literal enclosed in quotes or double quotes: escape sequences need to be parsed.
    std::string_view unquoted = input.substr(1, input.length() - 2);
    if (!unescapeStringLiteral(unquoted, result, error_message, error_pos))
    {
        ++error_pos;
        return false;
    }

    return true;
}

namespace
{
    /// Finds next underscore between two digits (or two hexadecimal digits if `is_hex` is true).
    /// The function returns String::npos if not found,
    size_t findUnderscoreBetweenDigits(std::string_view str, bool is_hex, size_t start_pos)
    {
        chassert(start_pos <= str.length());
        size_t pos = str.find('_', start_pos);
        while (pos != String::npos)
        {
            if ((1 <= pos) && (pos + 2 <= str.length()))
            {
                char before = str[pos - 1];
                char after = str[pos + 1];
                bool between_digits = is_hex ? (std::isxdigit(before) && std::isxdigit(after)) : (std::isdigit(before) && std::isdigit(after));
                if (between_digits)
                    break;
            }
            pos = str.find('_', pos + 1);
        }
        return pos;
    }

    /// Removes all underscores between digits (or two hexadecimal digits if `is_hex` is true).
    /// For example, the function converts "1000_000_000" to "1000000000", "0x23_F_B" to "0x23FB" (with is_hex == true).
    String removeUnderscoresBetweenDigits(std::string_view input, bool is_hex)
    {
        String result;
        result.reserve(input.length());
        size_t pos = 0;
        while (pos != input.length())
        {
            size_t underscore_pos = findUnderscoreBetweenDigits(input, is_hex, pos);
            if (underscore_pos == String::npos)
            {
                result.append(input.substr(pos));
                break;
            }
            result.append(input.substr(pos, underscore_pos - pos));
            pos = underscore_pos + 1;
        }
        return result;
    }

    /// Parses an unsigned scalar in number format, for example "1000" or "1_000" or "5.67" or "2e10" or "Inf" or "Nan".
    /// Underscores between digits are ignored.
    template <typename T>
    bool parseNumber(std::string_view input, UInt32 scale, T & result, String & error_message, size_t & error_pos)
    {
        /// Remove underscores between digits if necessary.
        String str = removeUnderscoresBetweenDigits(input, /* is_hex = */ false);

        if constexpr (is_decimal_field<T>)
        {
            ReadBufferFromMemory buf{input};
            UInt32 unread_scale = scale;
            typename T::ValueType x;
            if (!tryReadDecimalText(buf, x, DecimalUtils::max_precision<typename T::ValueType>, unread_scale) || !buf.eof())
            {
                error_message = fmt::format("Cannot parse number {}", quoteString(input));
                error_pos = 0;
                return false;
            }
            if (common::mulOverflow(x.value, DecimalUtils::scaleMultiplier<typename T::ValueType>(unread_scale), x.value))
            {
                error_message = fmt::format("Cannot parse number {}: It's too big", quoteString(input));
                error_pos = 0;
                return false;
            }
            result = T{x, scale};
        }
        else
        {
            if (!tryParse(result, str))
            {
                error_message = fmt::format("Cannot parse number {}", quoteString(input));
                error_pos = 0;
                return false;
            }
        }
        return true;
    }

    /// Whether this input is a hexadecimal number with prefix '0x' or "0X".
    bool isHexFormat(std::string_view input)
    {
        bool found_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (std::tolower(input[1]) == 'x');
        return found_hex_prefix;
    }

    /// Tries to parse an unsigned scalar in hex format, for example "0x23_F_B".
    /// The function recognizes prefixes "0x" and "0X" and ignores underscores between digits.
    /// If it succeeds the function returns true and sets `result`.
    /// If it fails the function returns false and sets either `allow_other_formats` or `error_pos` & `error_message`.
    template <typename T>
    bool parseNumberInHex(std::string_view input, UInt32 scale, T & result, String & error_message, size_t & error_pos)
    {
        bool found_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (std::tolower(input[1]) == 'x');
        if (!found_hex_prefix)
        {
            /// No prefix "0x" is in the `input`, but we can still try other scalar formats.
            error_message = fmt::format("Cannot parse hexadecimal number {}: Expected prefix '0x'", quoteString(input));
            error_pos = 0;
            return false;
        }

        /// Remove prefix "0x" and underscores between digits.
        std::string_view input_without_prefix = input.substr(2);
        String str = removeUnderscoresBetweenDigits(input_without_prefix, /* is_hex = */ true);

        /// Parse hexadecimal number.
        Int64 value;
        if (!tryParseIntInBase<16>(value, str))
        {
            error_message = fmt::format("Cannot parse hexadecimal number {}", quoteString(input_without_prefix));
            error_pos = 2;
            return false;
        }

        if constexpr (is_decimal<T>)
        {
            if (common::mulOverflow(value, DecimalUtils::scaleMultiplier<T>(scale), value))
            {
                error_message = fmt::format("Cannot parse number {}: It's too big", quoteString(input));
                error_pos = 0;
                return false;
            }

            result = T{value, scale};
        }
        else
        {
            result = static_cast<T>(value);
        }

        return true;
    }

    /// Whether this input represents a duration, i.e. it contains time units.
    bool isDurationFormat(std::string_view input)
    {
        bool found_time_unit = (input.find_first_of("ywdhms") != String::npos);
        return found_time_unit;
    }

    /// Tries to parse an unsigned scalar in duration format, for example "1y2w5d13h15m30s1ms".
    /// If it succeeds the function returns true and sets `result`.
    /// If it fails the function returns false and sets either `allow_other_formats` or `error_pos` & `error_message`.
    template <typename T>
    bool parseDuration(std::string_view input, UInt32 scale, T & result, String & error_message, size_t & error_pos)
    {
        Decimal64 current = 0;
        UInt32 current_scale = scale;

        Decimal64 previous_unit = 0;
        std::string_view previous_unit_name;

        /// Iterate through all {number, time unit} pairs.
        size_t pos = 0;
        while (pos != input.length())
        {
            size_t number_start_pos = pos;
            while (pos != input.length() && std::isdigit(input[pos]))
                ++pos;

            if (pos == number_start_pos)
            {
                error_message = fmt::format("Cannot parse time duration {}: Expected a number combined with a time unit, got {}",
                                            quoteString(input), quoteString(input.substr(pos)));
                error_pos = pos;
                return false;
            }

            Int64 number = 0;
            std::string_view number_as_str = input.substr(number_start_pos, pos - number_start_pos);
            if (!tryParse(number, number_as_str))
            {
                error_message = fmt::format("Cannot parse time duration {}: Number {} is too big", quoteString(input), number_as_str);
                error_pos = number_start_pos;
                return false;
            }

            size_t unit_start_pos = pos;
            while (pos != input.length() && !std::isdigit(input[pos]))
                ++pos;

            std::string_view unit_name = input.substr(unit_start_pos, pos - unit_start_pos);
            Decimal64 unit = 0;
            UInt32 unit_scale = 0;

            if (unit_name == "y")
                unit.value = 365ULL * 24 * 60 * 60;  /// 1y equals 365d (ignoring leap days)
            else if (unit_name == "w")
                unit.value = 7 * 24 * 60 * 60;  /// 1w equals 7d
            else if (unit_name == "d")
                unit.value = 24 * 60 * 60;  /// 1d equals 24h
            else if (unit_name == "h")
                unit.value = 60 * 60;  /// 1h equals 60m
            else if (unit_name == "m")
                unit.value = 60;  /// 1m equals 60s
            else if (unit_name == "s")
                unit.value = 1;  /// 1s equals 1000ms
            else if (unit_name == "ms")
            {
                /// milliseconds
                unit.value = 1;
                unit_scale = 3;
            }
            else
            {
                error_message = fmt::format("Cannot parse time duration {}: Expected one of the supported time units ('y', 'w', 'd', 'h', 'm', 's', 'ms'), got {}",
                                            quoteString(input), quoteString(unit_name));
                error_pos = unit_start_pos;
                return false;
            }

            if (unit_scale < current_scale)
            {
                unit.value *= DecimalUtils::scaleMultiplier<Int64>(current_scale - unit_scale);
            }
            else if (unit_scale > current_scale)
            {
                if constexpr (is_decimal<T>)
                {
                    unit.value /= DecimalUtils::scaleMultiplier<Int64>(unit_scale - current_scale);
                }
                else
                {
                    auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(unit_scale - current_scale);
                    if (common::mulOverflow(current.value, scale_multiplier, current.value))
                    {
                        error_message = fmt::format("Cannot parse time duration {}: It's too big", quoteString(input));
                        error_pos = 0;
                        return false;
                    }
                    previous_unit.value *= scale_multiplier;
                    current_scale = unit_scale;
                }
            }

            if (previous_unit && (unit >= previous_unit))
            {
                error_message = fmt::format("Cannot parse time duration {}: Time units must be ordered from the longest to the shortest: {} must appear before {}",
                                            quoteString(input), quoteString(unit_name), quoteString(previous_unit_name));
                error_pos = unit_start_pos;
                return false;
            }

            Decimal64 add;
            bool overflow = common::mulOverflow(number, unit.value, add.value) || common::addOverflow(add.value, current.value, current.value);
            if (overflow)
            {
                error_message = fmt::format("Cannot parse time duration {}: It's too big", quoteString(input));
                error_pos = 0;
                return false;
            }

            previous_unit = unit;
            previous_unit_name = unit_name;
        }

        /// There should be at least one number with a time unit.
        if (!previous_unit)
        {
            error_message = fmt::format("Cannot parse time duration {}: Expected numbers combined with time units", quoteString(input));
            error_pos = 0;
            return false;
        }

        if constexpr(is_decimal_field<T>)
            result = T{current, scale};
        else
            result = static_cast<T>(DecimalField<Decimal64>{current, scale});

        return true;
    }

    /// Parses a scalar which is either a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or a time duration in the promql format (e.g. 1y2w5d13h15m30s1ms).
    /// Underscores (_) can be used in between decimal or hexadecimal digits (they don't mean anything).
    template <typename T>
    bool parseScalarImpl(std::string_view input, bool allow_sign, UInt32 scale, T & result, String & error_message, size_t & error_pos)
    {
        size_t pos = 0;

        /// Parse a sign.
        bool negative = false;
        if (allow_sign)
        {
            if (input.starts_with('+'))
            {
                ++pos;
            }
            else if (input.starts_with('-'))
            {
                negative = true;
                ++pos;
            }
        }

        /// Spaces between a sign and number are allowed.
        while (pos != input.length() && std::isspace(input[pos]))
            ++pos;

        std::string_view unsigned_input = input.substr(pos);

        /// Parse an unsigned number in one of three formats.
        bool ok = false;
        if (isHexFormat(unsigned_input))
            ok = parseNumberInHex(unsigned_input, scale, result, error_message, error_pos);
        else if (isDurationFormat(unsigned_input))
            ok = parseDuration(unsigned_input, scale, result, error_message, error_pos);
        else
            ok = parseNumber(unsigned_input, scale, result, error_message, error_pos);

        if (!ok)
        {
            error_pos += pos;
            return false;
        }

        if (negative)
        {
            if constexpr (is_decimal_field<T>)
                result = T{-result.getValue(), result.getScale()};
            else
                result = -result;
        }

        return true;
    }
}

/// Parses a scalar literal.
bool PrometheusQueryParsingUtil::parseScalar(std::string_view input, ScalarType & res_scalar, String & error_message, size_t & error_pos)
{
    return parseScalarImpl(input, /* allow_sign = */ true, /* scale */ 0, res_scalar, error_message, error_pos);
}

/// Parses an offset which can be written after the "offset" keyword.
bool PrometheusQueryParsingUtil::parseOffset(std::string_view input, UInt32 scale, OffsetType & res_offset, String & error_message, size_t & error_pos)
{
    return parseScalarImpl(input, /* allow_sign = */ true, scale, res_offset, error_message, error_pos);
}

/// Parses a timestamp which can be written after '@' character (e.g. "@ 1609746000").
bool PrometheusQueryParsingUtil::parseTimestamp(std::string_view input, UInt32 scale, TimestampType & res_timestamp, String & error_message, size_t & error_pos)
{
    return parseScalarImpl(input, /* allow_sign = */ false, scale, res_timestamp, error_message, error_pos);
}

/// Parses a time range which is used in range selectors.
bool PrometheusQueryParsingUtil::parseTimeRange(std::string_view input, UInt32 scale, OffsetType & res_range, String & error_message, size_t & error_pos)
{
    /// Check opening and closing brackets.
    if (!input.starts_with('['))
    {
        error_message = fmt::format("Cannot parse time range {}: Expected an opening bracket [", quoteString(input));
        error_pos = 0;
        return false;
    }

    if (!input.ends_with(']'))
    {
        error_message = fmt::format("Cannot parse time range {}: Expected a closing bracket ]", quoteString(input));
        error_pos = input.length() - 1;
        return false;
    }

    /// Skip spaces.
    size_t start_pos = 1;
    while (start_pos != input.length() && std::isspace(start_pos))
    {
        ++start_pos;
    }
    size_t end_pos = input.length() - 1;
    while (end_pos != start_pos && std::isspace(end_pos - 1))
    {
        --end_pos;
    }

    /// Parse a scalar literal between the brackets.
    std::string_view range_as_str = input.substr(start_pos, end_pos - start_pos);
    if (!parseScalarImpl(range_as_str, /* allow_sign = */ false, scale, res_range, error_message, error_pos))
    {
        error_pos += start_pos;
        return false;
    }

    return true;
}

/// Parses a time range with an optional resolution which are used in subqueries.
bool PrometheusQueryParsingUtil::parseSubqueryRange(std::string_view input, UInt32 scale,
                                                    OffsetType & range, std::optional<OffsetType> & resolution,
                                                    String & error_message, size_t & error_pos)
{
    /// Check opening and closing brackets.
    if (!input.starts_with('['))
    {
        error_message = fmt::format("Cannot parse subquery range {}: Expected an opening bracket [", quoteString(input));
        error_pos = 0;
        return false;
    }

    if (!input.ends_with(']'))
    {
        error_message = fmt::format("Cannot parse subquery range {}: Expected a closing bracket ]", quoteString(input));
        error_pos = input.length() - 1;
        return false;
    }

    /// Find a colon between the brackets.
    size_t colon_pos = input.find(':', 1);
    if (colon_pos == String::npos)
    {
        error_message = fmt::format("Cannot parse subquery range {}: Expected a colon : in it", quoteString(input));
        error_pos = 0;
        return false;
    }

    /// Skip spaces.
    size_t range_start_pos = 1;
    while (range_start_pos != input.length() && std::isspace(range_start_pos))
    {
        ++range_start_pos;
    }
    size_t range_end_pos = colon_pos;
    while (range_end_pos != range_start_pos && std::isspace(range_end_pos - 1))
    {
        --range_end_pos;
    }
    size_t resolution_start_pos = colon_pos + 1;
    while (resolution_start_pos != input.length() && std::isspace(resolution_start_pos))
    {
        ++resolution_start_pos;
    }
    size_t resolution_end_pos = input.length() - 1;
    while (resolution_end_pos != resolution_start_pos && std::isspace(resolution_end_pos - 1))
    {
        --resolution_end_pos;
    }

    /// Parse two scalar literals before and after the colon. The second scalar literal is optional.
    std::string_view range_as_str = input.substr(range_start_pos, range_end_pos - range_start_pos);
    std::string_view resolution_as_str = input.substr(resolution_start_pos, resolution_end_pos - resolution_start_pos);
    if (!parseScalarImpl(range_as_str, /* allow_sign = */ false, scale, range, error_message, error_pos))
    {
        error_pos += range_start_pos;
        return false;
    }
    if (!resolution_as_str.empty() && !parseScalarImpl(resolution_as_str, /* allow_sign = */ false, scale, resolution.emplace(), error_message, error_pos))
    {
        error_pos += resolution_start_pos;
        return false;
    }
    return true;
}

bool PrometheusQueryParsingUtil::containsTimeUnits(std::string_view input)
{
    return isDurationFormat(input);
}

}
