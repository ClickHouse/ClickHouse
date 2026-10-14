#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <Common/UTF8Helpers.h>
#include <Common/quoteString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readDecimalText.h>
#include <IO/readIntText.h>
#include <boost/algorithm/string/predicate.hpp>


namespace DB
{

namespace
{
    template<class... Types>
    void setErrorMessage(String * error_message, fmt::format_string<Types...> format, Types&&... args)
    {
        if (error_message)
            *error_message = fmt::format(format, std::forward<Types>(args)...);
    }

    void setErrorPos(size_t * error_pos, size_t value)
    {
        if (error_pos)
            *error_pos = value;
    }

    /// Parses escape sequences in a string literal and replaces them with the characters which they mean.
    bool tryUnescapeStringLiteral(std::string_view input, String & res_string, String * error_message, size_t * error_pos)
    {
        res_string.clear();
        res_string.reserve(input.length());

        for (size_t pos = 0; pos < input.length();)
        {
            size_t next_pos = input.find('\\', pos);
            if (next_pos == String::npos)
                next_pos = input.length();

            res_string.append(input.substr(pos, next_pos - pos));
            pos = next_pos;

            if (pos >= input.length())
                break;

            /// An escape sequences contains at least 2 characters.
            if (pos + 2 > input.length())
            {
                setErrorMessage(error_message,
                                "Invalid escape sequence {}: Expected at least 2 characters",
                                quoteString(input.substr(pos)));
                setErrorPos(error_pos, pos);
                return false;
            }

            chassert(input[pos] == '\\');
            char c = input[pos + 1];

            switch (c)
            {
                case 'a':  res_string.push_back(0x07); pos += 2; break;  /// \a  U+0007 alert or bell
                case 'b':  res_string.push_back(0x08); pos += 2; break;  /// \b  U+0008 backspace
                case 'f':  res_string.push_back(0x0C); pos += 2; break;  /// \f  U+000C form feed
                case 'n':  res_string.push_back(0x0A); pos += 2; break;  /// \n  U+000A line feed or newline
                case 'r':  res_string.push_back(0x0D); pos += 2; break;  /// \r  U+000D carriage return
                case 't':  res_string.push_back(0x09); pos += 2; break;  /// \t  U+0009 horizontal tab
                case 'v':  res_string.push_back(0x0B); pos += 2; break;  /// \v  U+000B vertical tab
                case '\\': res_string.push_back('\\'); pos += 2; break;  /// \\  U+005C backslash
                case '\'': res_string.push_back('\''); pos += 2; break;  /// \'  U+0027 single quote
                case '"':  res_string.push_back('"');  pos += 2; break;  /// \"  U+0022 double quote
                case 'x':
                {
                    /// \x followed by exactly two hexadecimal digits represents a single byte.
                    /// Example: \x51 is the 'Q' letter.
                    if (pos + 4 > input.length())
                    {
                        setErrorMessage(error_message, "Invalid escape sequence {}: Expected 4 characters", quoteString(input.substr(pos)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    UInt8 byte;
                    if (!tryParseIntInBase<16>(byte, input.substr(pos + 2, 2)))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: Cannot parse a hexadecimal number representing a single byte",
                                        quoteString(input.substr(pos, 4)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    res_string.push_back(byte);
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
                        setErrorMessage(error_message, "Invalid escape sequence {}: Expected 4 characters", quoteString(input.substr(pos)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    UInt16 byte;
                    if (!tryParseIntInBase<8>(byte, input.substr(pos + 1, 3)))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: Cannot parse an octal number representing a single byte",
                                        quoteString(input.substr(pos, 4)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    if (byte > 0xFF)
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: An octal representation \nnn must represent a single byte",
                                        quoteString(input.substr(pos, 4)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    res_string.push_back(static_cast<char>(byte));
                    pos += 4;
                    break;
                }
                case 'u':
                {
                    /// \u followed by exactly four hexadecimal digits represents a single Unicode code point.
                    /// Example: \u0051 is the 'Q' letter.
                    if (pos + 6 > input.length())
                    {
                        setErrorMessage(error_message, "Invalid escape sequence {}: Expected 6 characters", quoteString(input.substr(pos)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    UInt16 code_point;
                    if (!tryParseIntInBase<16>(code_point, input.substr(pos + 2, 4)))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: Cannot parse a hexadecimal number representing a Unicode code point",
                                        quoteString(input.substr(pos, 6)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    char bytes[3];  /// 3 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    res_string.append(bytes, num_bytes);
                    pos += 6;
                    break;
                }
                case 'U':
                {
                    /// \U followed by exactly eight hexadecimal digits represents a single Unicode code point.
                    /// Example: \U00000051 is the 'Q' letter.
                    if (pos + 10 > input.length())
                    {
                        setErrorMessage(
                            error_message, "Invalid escape sequence {}: Expected 10 characters", quoteString(input.substr(pos)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    UInt32 code_point;
                    if (!tryParseIntInBase<16>(code_point, input.substr(pos + 2, 8)))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: Cannot parse a hexadecimal number representing a Unicode code point",
                                        quoteString(input.substr(pos, 10)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    if (code_point > 0x10FFFF)  /// There should be no Unicode code point beyond 0x10FFFF.
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: A Unicode code point can't be greater than 0x10FFFF",
                                        quoteString(input.substr(pos, 10)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    char bytes[4];  /// 4 bytes is enough to represent a Unicode code point up to 0xFFFF.
                    size_t num_bytes = UTF8::convertCodePointToUTF8(code_point, bytes, sizeof(bytes));
                    res_string.append(bytes, num_bytes);
                    pos += 10;
                    break;
                }
                default:
                {
                    setErrorMessage(error_message, "Invalid escape sequence {}", quoteString(input.substr(pos)));
                    setErrorPos(error_pos, pos);
                    return false;
                }
            }
        }
        return true;
    }
}

/// Converts a quoted string literal to its unquoted version.
bool PrometheusQueryParsingUtil::tryParseStringLiteral(
    std::string_view input, String & res_string, String * error_message, size_t * error_pos)
{
    res_string.clear();

    if (!input.starts_with('\'') && !input.starts_with('\"') && !input.starts_with('`'))
    {
        setErrorMessage(error_message,
                        "Cannot parse string literal {}: A string literal must open with a quote ', a double quote \" or a backtick `",
                        input);
        setErrorPos(error_pos, 0);
        return false;
    }

    char quote_char = input[0];

    if ((input.length() < 2) || !input.ends_with(quote_char))
    {
        std::string_view quote_char_name = (quote_char == '\'') ? "quote" : ((quote_char == '\"') ? "double quote" : "backtick");
        setErrorMessage(error_message, "Cannot parse string literal {}: No closing {} {}", input, quote_char_name, quote_char);
        setErrorPos(error_pos, input.length() - 1);
        return false;
    }

    /// A string literal enclosed in backticks: escape sequences are not parsed.
    if (quote_char == '`')
    {
        size_t closing_backtick = input.find('`', 1);
        if (closing_backtick != input.length() - 1)
        {
            setErrorMessage(error_message, "Cannot parse string literal {}: A string literal in backticks can't contain other backticks", input);
            setErrorPos(error_pos, closing_backtick);
            return false;
        }
        res_string = input.substr(1, input.length() - 2);
        return true;
    }

    /// A string literal enclosed in quotes or double quotes: escape sequences need to be parsed.
    std::string_view unquoted = input.substr(1, input.length() - 2);
    if (!tryUnescapeStringLiteral(unquoted, res_string, error_message, error_pos))
    {
        if (error_pos)
            ++*error_pos;
        return false;
    }

    return true;
}

namespace
{
    /// Finds next underscore between two digits, or two hexadecimal digits if `is_hex` is true.
    /// Also we allow an underscore between prefix "0x" and hexadecimal digits, for example "0x_1_2_3" is allowed
    /// (because it's allowed in Prometheus).
    /// The function returns String::npos if not found.
    template <bool is_hex>
    size_t findUnderscoreBetweenDigits(std::string_view str, size_t start_pos)
    {
        chassert(start_pos <= str.length());
        size_t pos = str.find('_', start_pos);
        while (pos != String::npos)
        {
            if ((1 <= pos) && (pos + 2 <= str.length()))
            {
                char before = str[pos - 1];
                char after = str[pos + 1];

                bool between_digits;
                if constexpr (is_hex)
                    between_digits = (std::isxdigit(before) || (std::tolower(before) == 'x')) && std::isxdigit(after);
                else
                    between_digits = std::isdigit(before) && std::isdigit(after);

                if (between_digits)
                    break;
            }
            pos = str.find('_', pos + 1);
        }
        return pos;
    }

    /// Removes all underscores between digits (or two hexadecimal digits if `is_hex` is true).
    /// For example, the function converts "1000_000_000" to "1000000000", "0x23_F_B" to "0x23FB" (with is_hex == true).
    template <bool is_hex>
    String removeUnderscoresBetweenDigits(std::string_view input)
    {
        String result;
        result.reserve(input.length());
        size_t pos = 0;
        while (pos != input.length())
        {
            size_t underscore_pos = findUnderscoreBetweenDigits<is_hex>(input, pos);
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

    using ScalarType = PrometheusQueryParsingUtil::ScalarType;
    using TimestampType = PrometheusQueryParsingUtil::TimestampType;
    using DurationType = PrometheusQueryParsingUtil::DurationType;

    template <typename T>
    std::string_view getTypeName()
    {
        if constexpr (std::is_same_v<T, TimestampType>)
            return "timestamp";
        else if constexpr (std::is_same_v<T, DurationType>)
            return "duration";
        else
            return "number";
    }

    /// Parses an unsigned scalar in number format, for example "1000" or "1_000" or "5.67" or "2e10" or "Inf" or "Nan".
    /// Underscores between digits are ignored.
    template <typename T>
    bool tryParseNumberFormat(std::string_view input, UInt32 scale, T & result, String * error_message, size_t * error_pos)
    {
        /// Remove underscores between digits if necessary.
        String str = removeUnderscoresBetweenDigits</* is_hex = */ false>(input);

        if constexpr (is_decimal<T>)
        {
            if (boost::iequals(input, "Inf") || boost::iequals(input, "NaN"))
            {
                setErrorMessage(error_message, "Cannot parse {} {}: Should be finite", getTypeName<T>(), quoteString(input));
                setErrorPos(error_pos, 0);
                return false;
            }

            ReadBufferFromString buf(str);

            UInt32 target_scale = scale;
            if (!tryReadDecimalText(buf, result, DecimalUtils::max_precision<T>, target_scale))
            {
                setErrorMessage(error_message, "Cannot parse {} {}", getTypeName<T>(), quoteString(str));
                setErrorPos(error_pos, 0);
                return false;
            }

            /// tryReadDecimalText() has already checked for overflow.
            result *= DecimalUtils::scaleMultiplier<Decimal64>(target_scale);
        }
        else
        {
            if (!tryParse(result, str))
            {
                setErrorMessage(error_message, "Cannot parse {} {}", getTypeName<T>(), quoteString(str));
                setErrorPos(error_pos, 0);
                return false;
            }
        }

        return true;
    }

    /// Whether this input is a hexadecimal number with prefix '0x' or "0X".
    bool isHexFormat(std::string_view input)
    {
        bool has_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (std::tolower(input[1]) == 'x');
        return has_hex_prefix;
    }

    /// Tries to parse an unsigned scalar in hex format, for example "0x23_F_B".
    /// The function recognizes prefixes "0x" and "0X" and ignores underscores between digits.
    /// If it succeeds the function returns true and sets `result`.
    /// If it fails the function returns false and sets either `allow_other_formats` or `error_pos` & `error_message`.
    template <typename T>
    bool tryParseHexFormat(std::string_view input, UInt32 scale, T & result, String * error_message, size_t * error_pos)
    {
        bool has_hex_prefix = (input.length() >= 2) && (input[0] == '0') && (std::tolower(input[1]) == 'x');
        if (!has_hex_prefix)
        {
            /// No prefix "0x" is in the `input`, but we can still try other scalar formats.
            setErrorMessage(error_message, "Cannot parse {} {} in hexadecimal format: Expected prefix '0x'", getTypeName<T>(), quoteString(input));
            setErrorPos(error_pos, 0);
            return false;
        }

        /// Remove prefix "0x" and underscores between digits.
        String str = removeUnderscoresBetweenDigits</* is_hex = */ true>(input);
        std::string_view hex_without_prefix = std::string_view{str}.substr(2);

        /// Parse hexadecimal number.
        Int64 value;
        if (!tryParseIntInBase<16>(value, hex_without_prefix))
        {
            setErrorMessage(error_message, "Cannot parse {} {} in hexadecimal format", getTypeName<T>(), quoteString(str));
            setErrorPos(error_pos, 2);
            return false;
        }

        if constexpr (is_decimal<T>)
        {
            if (common::mulOverflow(value, DecimalUtils::scaleMultiplier<T>(scale), result.value))
            {
                setErrorMessage(error_message, "Cannot parse {} {} in hexadecimal format: Overflow, the number is too big", getTypeName<T>(), quoteString(input));
                setErrorPos(error_pos, 0);
                return false;
            }
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
    bool tryParseDurationFormat(std::string_view input, UInt32 scale, T & result, String * error_message, size_t * error_pos)
    {
        bool has_time_units = false;
        Int64 seconds = 0;
        Int64 milliseconds = 0;

        /// Iterate through all {number, time unit} pairs.
        size_t pos = 0;
        while (pos != input.length())
        {
            size_t number_start_pos = pos;
            while (pos != input.length() && std::isdigit(input[pos]))
                ++pos;

            if (pos == number_start_pos)
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {} in duration format: Expected a number combined with a time unit, got {}",
                                getTypeName<T>(), quoteString(input), quoteString(input.substr(pos)));
                setErrorPos(error_pos, pos);
                return false;
            }

            Int64 number = 0;
            std::string_view number_as_str = input.substr(number_start_pos, pos - number_start_pos);
            if (!tryParse(number, number_as_str))
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {} in duration format: Overflow, the number {} is too big",
                                getTypeName<T>(), quoteString(input), number_as_str);
                setErrorPos(error_pos, number_start_pos);
                return false;
            }

            size_t unit_start_pos = pos;
            while (pos != input.length() && !std::isdigit(input[pos]))
                ++pos;

            std::string_view unit_name = input.substr(unit_start_pos, pos - unit_start_pos);
            has_time_units = true;

            Int64 seconds_per_unit = 0;
            Int64 ms_per_unit = 0;

            if (unit_name == "y")
                seconds_per_unit = 365ULL * 24 * 60 * 60;  /// 1y equals 365d (ignoring leap days)
            else if (unit_name == "w")
                seconds_per_unit = 7 * 24 * 60 * 60;  /// 1w equals 7d
            else if (unit_name == "d")
                seconds_per_unit = 24 * 60 * 60;  /// 1d equals 24h
            else if (unit_name == "h")
                seconds_per_unit = 60 * 60;  /// 1h equals 60m
            else if (unit_name == "m")
                seconds_per_unit = 60;  /// 1m equals 60s
            else if (unit_name == "s")
                seconds_per_unit = 1;  /// 1s equals 1000ms
            else if (unit_name == "ms")
                ms_per_unit = 1;
            else
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {} in duration format: Expected one of the supported time units ('y', 'w', 'd', 'h', 'm', 's', 'ms'), got {}",
                                getTypeName<T>(), quoteString(input), quoteString(unit_name));
                setErrorPos(error_pos, unit_start_pos);
                return false;
            }

            if (seconds_per_unit)
            {
                if (!DecimalUtils::tryMultiplyAdd(number, seconds_per_unit, seconds, seconds))
                {
                    setErrorMessage(error_message,
                                    "Cannot parse {} {} in duration format: Overflow, the duration is too big",
                                    getTypeName<T>(), quoteString(input));
                    setErrorPos(error_pos, 0);
                    return false;
                }
            }

            if (ms_per_unit)
            {
                if (!DecimalUtils::tryMultiplyAdd(number, ms_per_unit, milliseconds, milliseconds))
                {
                    setErrorMessage(error_message,
                                    "Cannot parse {} {} in duration format: Overflow, the duration is too big",
                                    getTypeName<T>(), quoteString(input));
                    setErrorPos(error_pos, 0);
                    return false;
                }
            }
        }

        /// There should be at least one number with a time unit.
        if (!has_time_units)
        {
            setErrorMessage(error_message,
                            "Cannot parse {} {} in duration format: Expected numbers combined with time units",
                            getTypeName<T>(), quoteString(input));
            setErrorPos(error_pos, 0);
            return false;
        }

        if constexpr (is_decimal<T>)
        {
            Int64 scaled_milliseconds;
            if (scale > 3)
            {
                if (common::mulOverflow(milliseconds, DecimalUtils::scaleMultiplier<T>(scale - 3), scaled_milliseconds))
                {
                    setErrorMessage(error_message,
                                    "Cannot parse {} {}: Overflow, the duration is too big",
                                    getTypeName<T>(), quoteString(input));
                    setErrorPos(error_pos, 0);
                    return false;
                }
            }
            else if (scale < 3)
            {
                Int64 divisor = DecimalUtils::scaleMultiplier<T>(3 - scale);
                scaled_milliseconds = milliseconds / divisor;
            }
            else
            {
                scaled_milliseconds = milliseconds;
            }

            if (!DecimalUtils::tryMultiplyAdd(seconds, DecimalUtils::scaleMultiplier<T>(scale), scaled_milliseconds, result.value))
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {}: Overflow, the duration is too big",
                                getTypeName<T>(), quoteString(input));
                setErrorPos(error_pos, 0);
                return false;
            }
        }
        else
        {
            result = static_cast<ScalarType>(seconds) + static_cast<ScalarType>(milliseconds) / 1000;
        }

        return true;
    }

    template <typename T>
    bool tryParseNumber(std::string_view input, UInt32 scale, T & result, String * error_message, size_t * error_pos)
    {
        size_t pos = 0;

        /// Parse a sign.
        bool negative = false;
        if (input.starts_with('+'))
        {
            ++pos;
        }
        else if (input.starts_with('-'))
        {
            negative = true;
            ++pos;
        }

        /// Spaces between a sign and number are allowed.
        while (pos != input.length() && std::isspace(input[pos]))
            ++pos;

        size_t end_pos = input.length();
        while (end_pos != pos && std::isspace(input[end_pos - 1]))
            --end_pos;

        std::string_view unsigned_input = input.substr(pos, end_pos - pos);

        bool ok = false;

        /// Parse an unsigned number in one of three formats.
        if (isHexFormat(unsigned_input))
        {
            ok = tryParseHexFormat(unsigned_input, scale, result, error_message, error_pos);
        }
        else if (isDurationFormat(unsigned_input))
        {
            ok = tryParseDurationFormat(unsigned_input, scale, result, error_message, error_pos);
        }
        else
        {
            ok = tryParseNumberFormat(unsigned_input, scale, result, error_message, error_pos);
        }

        if (!ok)
        {
            if (error_pos)
                *error_pos += pos;
            return false;
        }

        if (negative)
            result = -result;

        return true;
    }
}


bool PrometheusQueryParsingUtil::tryParseScalar(std::string_view input, ScalarType & res_scalar, String * error_message, size_t * error_pos)
{
    /// Here `scale` is set to `0` because it's unused when parsing a floating-point number.
    return tryParseNumber(input, /* scale */ 0, res_scalar, error_message, error_pos);
}

bool PrometheusQueryParsingUtil::tryParseTimestamp(
    std::string_view input, UInt32 timestamp_scale, TimestampType & res_timestamp, String * error_message, size_t * error_pos)
{
    return tryParseNumber(input, timestamp_scale, res_timestamp, error_message, error_pos);
}

bool PrometheusQueryParsingUtil::tryParseDuration(
    std::string_view input, UInt32 timestamp_scale, DurationType & res_duration, String * error_message, size_t * error_pos)
{
    return tryParseNumber(input, timestamp_scale, res_duration, error_message, error_pos);
}


/// Parses a time range which is used in range selectors.
bool PrometheusQueryParsingUtil::tryParseSelectorRange(
    std::string_view input, UInt32 timestamp_scale, DurationType & res_range, String * error_message, size_t * error_pos)
{
    /// Check opening and closing brackets.
    if (!input.starts_with('['))
    {
        setErrorMessage(error_message, "Cannot parse time range {}: Expected an opening bracket [", quoteString(input));
        setErrorPos(error_pos, 0);
        return false;
    }

    if (!input.ends_with(']'))
    {
        setErrorMessage(error_message, "Cannot parse time range {}: Expected a closing bracket ]", quoteString(input));
        setErrorPos(error_pos, input.length() - 1);
        return false;
    }

    /// Skip spaces.
    size_t start_pos = 1;
    while (start_pos != input.length() && std::isspace(input[start_pos]))
    {
        ++start_pos;
    }
    size_t end_pos = input.length() - 1;
    while (end_pos != start_pos && std::isspace(input[end_pos - 1]))
    {
        --end_pos;
    }

    if (start_pos == end_pos)
    {
        setErrorMessage(error_message, "Cannot parse time range {}: Expected a duration between brackets [ ]", quoteString(input));
        setErrorPos(error_pos, start_pos);
        return false;
    }

    if (!tryParseDuration(input.substr(start_pos, end_pos - start_pos), timestamp_scale, res_range, error_message, error_pos))
    {
        if (error_pos)
            *error_pos += start_pos;
        return false;
    }

    return true;
}

/// Parses a time range with an optional step which are used in subqueries.
bool PrometheusQueryParsingUtil::tryParseSubqueryRange(
    std::string_view input,
    UInt32 timestamp_scale,
    DurationType & res_range,
    std::optional<DurationType> & res_step,
    String * error_message,
    size_t * error_pos)
{
    /// Check opening and closing brackets.
    if (!input.starts_with('['))
    {
        setErrorMessage(error_message, "Cannot parse subquery range {}: Expected an opening bracket [", quoteString(input));
        setErrorPos(error_pos, 0);
        return false;
    }

    if (!input.ends_with(']'))
    {
        setErrorMessage(error_message, "Cannot parse subquery range {}: Expected a closing bracket ]", quoteString(input));
        setErrorPos(error_pos, input.length() - 1);
        return false;
    }

    /// Find a colon between the brackets.
    size_t colon_pos = input.find(':', 1);
    if (colon_pos == String::npos)
    {
        setErrorMessage(error_message, "Cannot parse subquery range {}: Expected a colon : in it", quoteString(input));
        setErrorPos(error_pos, 0);
        return false;
    }

    /// Skip spaces.
    size_t range_start_pos = 1;
    while (range_start_pos != input.length() && std::isspace(input[range_start_pos]))
    {
        ++range_start_pos;
    }
    size_t range_end_pos = colon_pos;
    while (range_end_pos != range_start_pos && std::isspace(input[range_end_pos - 1]))
    {
        --range_end_pos;
    }
    size_t step_start_pos = colon_pos + 1;
    while (step_start_pos != input.length() && std::isspace(input[step_start_pos]))
    {
        ++step_start_pos;
    }
    size_t step_end_pos = input.length() - 1;
    while (step_end_pos != step_start_pos && std::isspace(input[step_end_pos - 1]))
    {
        --step_end_pos;
    }

    if (range_start_pos == range_end_pos)
    {
        setErrorMessage(error_message, "Cannot parse time range {}: Expected a duration between opening bracket [ and colon :", quoteString(input));
        setErrorPos(error_pos, range_start_pos);
        return false;
    }

    if (!tryParseDuration(input.substr(range_start_pos, range_end_pos - range_start_pos), timestamp_scale, res_range, error_message, error_pos))
    {
        if (error_pos)
            *error_pos += range_start_pos;
        return false;
    }

    res_step.reset();

    if (step_start_pos != step_end_pos)
    {
        if (!tryParseDuration(input.substr(step_start_pos, step_end_pos - step_start_pos), timestamp_scale, res_step.emplace(), error_message, error_pos))
        {
            if (error_pos)
                *error_pos += step_start_pos;
            return false;
        }
    }

    return true;
}

}
