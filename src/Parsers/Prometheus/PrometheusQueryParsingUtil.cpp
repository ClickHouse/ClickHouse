#include <Common/StringUtils.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/UTF8Helpers.h>
#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/readDecimalText.h>
#include <IO/readIntText.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>


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
    bool tryUnescapeStringLiteral(
        std::string_view input, char quote_char, String & res_string, String * error_message, size_t * error_pos)
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
                case '\'':
                case '"':
                {
                    if (c != quote_char)
                    {
                        setErrorMessage(error_message, "Invalid escape sequence {}", quoteString(input.substr(pos)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    res_string.push_back(c);
                    pos += 2;
                    break;
                }
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
                    UInt8 byte = 0;
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
                    UInt16 byte = 0;
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
                    UInt16 code_point = 0;
                    if (!tryParseIntInBase<16>(code_point, input.substr(pos + 2, 4)))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: Cannot parse a hexadecimal number representing a Unicode code point",
                                        quoteString(input.substr(pos, 6)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    if (UTF8::isSurrogateCodePoint(code_point))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: A Unicode code point can't be in the surrogate range 0xD800-0xDFFF",
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
                    UInt32 code_point = 0;
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
                    if (UTF8::isSurrogateCodePoint(code_point))
                    {
                        setErrorMessage(error_message,
                                        "Invalid escape sequence {}: A Unicode code point can't be in the surrogate range 0xD800-0xDFFF",
                                        quoteString(input.substr(pos, 10)));
                        setErrorPos(error_pos, pos);
                        return false;
                    }
                    char bytes[4];  /// 4 bytes is enough to represent a Unicode code point up to 0x10FFFF.
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
    const size_t newline_pos = unquoted.find('\n');
    size_t unescape_error_pos = 0;
    const bool unescape_succeeded = tryUnescapeStringLiteral(unquoted, quote_char, res_string, error_message, &unescape_error_pos);

    if (newline_pos != String::npos && (unescape_succeeded || newline_pos < unescape_error_pos))
    {
        setErrorMessage(error_message, "unterminated quoted string");
        setErrorPos(error_pos, 0);
        return false;
    }

    if (!unescape_succeeded)
    {
        setErrorPos(error_pos, unescape_error_pos + 1);
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

                bool between_digits = false;
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
    using RequestTimestampType = PrometheusQueryParsingUtil::RequestTimestampType;
    using DurationType = PrometheusQueryParsingUtil::DurationType;

    bool tryParsePrometheusRequestTimestampFloat(std::string_view input, Float64 & result)
    {
        if (input.empty() || input.find_first_of(" \t\n\r\f\v") != std::string_view::npos)
            return false;

        size_t number_start = 0;
        if (input[number_start] == '+' || input[number_start] == '-')
            ++number_start;
        if (number_start >= input.size())
            return false;

        const bool is_hex = number_start + 2 <= input.size()
            && input[number_start] == '0'
            && (input[number_start + 1] == 'x' || input[number_start + 1] == 'X');
        if (is_hex && input.find_first_of("pP", number_start + 2) == std::string_view::npos)
            return false;

        const String normalized = is_hex
            ? removeUnderscoresBetweenDigits<true>(input)
            : removeUnderscoresBetweenDigits<false>(input);

        if (is_hex)
        {
            char * end = nullptr;
            result = std::strtod(normalized.c_str(), &end);
            if (end != normalized.data() + normalized.size())
                return false;
        }
        else if (!tryParse(result, normalized))
        {
            return false;
        }

        return std::isfinite(result);
    }

    bool isPrometheusRequestTimestampNumber(std::string_view input)
    {
        Float64 result = 0;
        return tryParsePrometheusRequestTimestampFloat(input, result);
    }

    bool isValidRFC3339Date(UInt32 year, UInt32 month, UInt32 day)
    {
        if (month == 0 || month > 12 || day == 0)
            return false;

        UInt32 days_in_month = 31;
        switch (month)
        {
            case 4:
            case 6:
            case 9:
            case 11:
                days_in_month = 30;
                break;
            case 2:
                days_in_month = 28;
                if ((year % 400 == 0) || (year % 100 != 0 && year % 4 == 0))
                    ++days_in_month;
                break;
            default:
                break;
        }

        return day <= days_in_month;
    }

    bool tryParseFloatInteger(Float64 input, Int128 & result)
    {
        std::array<char, 512> buffer{};
        const auto [end, error] = std::to_chars(
            buffer.data(), buffer.data() + buffer.size(), input, std::chars_format::fixed, /* precision */ 0);
        if (error != std::errc())
            return false;

        return tryParseInt(result, std::string_view(buffer.data(), end - buffer.data()));
    }

    /// Parse a numeric HTTP timestamp through the same Float64 -> fractional milliseconds path as
    /// Prometheus' parseTime. Keep the result in Decimal128 after rounding so range validation can
    /// still distinguish values outside the storage domain before converting them to its timestamp type.
    bool tryParsePrometheusRequestTimestampNumber(
        std::string_view input, UInt32 timestamp_scale, RequestTimestampType & result)
    {
        Float64 timestamp = 0;
        if (!tryParsePrometheusRequestTimestampFloat(input, timestamp))
            return false;

        Float64 whole_seconds_float = 0;
        const Float64 fractional_seconds = std::modf(timestamp, &whole_seconds_float);
        const auto rounded_milliseconds = static_cast<Int64>(std::round(fractional_seconds * 1000));

        Int128 whole_seconds = 0;
        if (!tryParseFloatInteger(whole_seconds_float, whole_seconds))
            return false;

        Int64 milliseconds = rounded_milliseconds;
        if (milliseconds == 1000)
        {
            if (whole_seconds == std::numeric_limits<Int128>::max())
                return false;
            ++whole_seconds;
            milliseconds = 0;
        }
        else if (milliseconds == -1000)
        {
            if (whole_seconds == std::numeric_limits<Int128>::min())
                return false;
            --whole_seconds;
            milliseconds = 0;
        }

        const auto scale_multiplier = DecimalUtils::scaleMultiplier<Int128>(timestamp_scale);
        Int128 scaled_milliseconds = milliseconds;
        if (timestamp_scale > 3)
            scaled_milliseconds *= DecimalUtils::scaleMultiplier<Int128>(timestamp_scale - 3);
        else if (timestamp_scale < 3)
            scaled_milliseconds /= DecimalUtils::scaleMultiplier<Int128>(3 - timestamp_scale);

        return DecimalUtils::tryMultiplyAdd(
            whole_seconds, scale_multiplier, scaled_milliseconds, result.value);
    }

    /// Prometheus accepts these two values as special cases because Go's RFC3339 parser only accepts
    /// four-digit years. Keep the same wire forms and represent them in the request timestamp scale;
    /// appendTimeRangeConditions() will clip them to the actual TimeSeries storage domain later.
    constexpr std::string_view prometheus_min_time_string = "-292273086-05-16T16:47:06Z";
    constexpr std::string_view prometheus_max_time_string = "292277025-08-18T07:12:54.999999999Z";
    constexpr Int128 prometheus_min_time_seconds
        = static_cast<Int128>(std::numeric_limits<Int64>::min()) / 1000 + 62135596801;
    constexpr Int128 prometheus_max_time_seconds
        = static_cast<Int128>(std::numeric_limits<Int64>::max()) / 1000 - 62135596801;

    bool tryParsePrometheusRequestTimestampBoundary(
        std::string_view input, UInt32 timestamp_scale, RequestTimestampType & result)
    {
        Int128 seconds = 0;
        UInt32 nanoseconds = 0;
        if (input == prometheus_min_time_string)
        {
            seconds = prometheus_min_time_seconds;
        }
        else if (input == prometheus_max_time_string)
        {
            seconds = prometheus_max_time_seconds;
            nanoseconds = 999'999'999;
        }
        else
        {
            return false;
        }

        const auto scale_multiplier = DecimalUtils::scaleMultiplier<Int128>(timestamp_scale);
        Int128 fractional = nanoseconds;
        if (timestamp_scale > 9)
            fractional *= DecimalUtils::scaleMultiplier<Int128>(timestamp_scale - 9);
        else if (timestamp_scale < 9)
            fractional /= DecimalUtils::scaleMultiplier<Int128>(9 - timestamp_scale);

        result.value = seconds * scale_multiplier + fractional;
        return true;
    }

    /// Parse the RFC3339 form accepted by time.Parse(time.RFC3339Nano, ...). The
    /// DateTime parser has useful calendar validation and UTC conversion, but accepts many
    /// ClickHouse-specific forms such as date-only and space-separated timestamps. Validate the
    /// wire grammar here before using DateLUT for the calendar conversion.
    bool tryParsePrometheusRFC3339Timestamp(
        std::string_view input, UInt32 timestamp_scale, RequestTimestampType & result)
    {
        if (tryParsePrometheusRequestTimestampBoundary(input, timestamp_scale, result))
            return true;

        constexpr size_t date_time_prefix_size = 19; /// YYYY-MM-DDTHH:MM:SS
        if (input.size() <= date_time_prefix_size)
            return false;

        const auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
        for (size_t i : {0uz, 1uz, 2uz, 3uz, 5uz, 6uz, 8uz, 9uz, 11uz, 12uz, 14uz, 15uz, 17uz, 18uz})
        {
            if (!is_digit(input[i]))
                return false;
        }

        if (input[4] != '-' || input[7] != '-' || input[10] != 'T' || input[13] != ':' || input[16] != ':')
            return false;

        const auto read_two_digits = [&](size_t pos)
        {
            return static_cast<UInt32>((input[pos] - '0') * 10 + input[pos + 1] - '0');
        };

        const UInt32 year = static_cast<UInt32>((input[0] - '0') * 1000 + (input[1] - '0') * 100 + (input[2] - '0') * 10 + input[3] - '0');
        const UInt32 month = read_two_digits(5);
        const UInt32 day = read_two_digits(8);
        const UInt32 hour = read_two_digits(11);
        const UInt32 minute = read_two_digits(14);
        const UInt32 second = read_two_digits(17);

        /// Match time.Parse's range checks for the local clock and calendar fields. Go accepts
        /// offsets up to 24:60, so retain that behavior here too.
        if (!isValidRFC3339Date(year, month, day) || hour >= 24 || minute >= 60 || second >= 60)
            return false;

        size_t pos = date_time_prefix_size;
        size_t fraction_start = pos;
        size_t fraction_end = pos;
        if (input[pos] == '.' || input[pos] == ',')
        {
            fraction_start = ++pos;
            while (pos < input.size() && is_digit(input[pos]))
                ++pos;
            if (pos == fraction_start)
                return false;
            fraction_end = pos;
        }

        UInt32 offset_hours = 0;
        UInt32 offset_minutes = 0;
        bool negative_offset = false;
        if (pos < input.size() && input[pos] == 'Z')
        {
            ++pos;
        }
        else if (pos < input.size() && (input[pos] == '+' || input[pos] == '-'))
        {
            negative_offset = input[pos] == '-';
            if (input.size() != pos + 6 || !is_digit(input[pos + 1]) || !is_digit(input[pos + 2]) || input[pos + 3] != ':'
                || !is_digit(input[pos + 4]) || !is_digit(input[pos + 5]))
                return false;

            offset_hours = read_two_digits(pos + 1);
            offset_minutes = read_two_digits(pos + 4);
            if (offset_hours > 24 || offset_minutes > 60)
                return false;
            pos += 6;
        }
        else
        {
            return false;
        }

        if (pos != input.size())
            return false;

        const auto seconds_since_epoch = DateLUT::instance("UTC").tryToMakeDateTime(
            static_cast<Int16>(year),
            static_cast<UInt8>(month),
            static_cast<UInt8>(day),
            static_cast<UInt8>(hour),
            static_cast<UInt8>(minute),
            static_cast<UInt8>(second));
        if (!seconds_since_epoch)
            return false;

        Int128 utc_seconds = static_cast<Int128>(*seconds_since_epoch);
        const Int128 offset_seconds = static_cast<Int128>(offset_hours) * 3600 + static_cast<Int128>(offset_minutes) * 60;
        if (negative_offset)
            utc_seconds += offset_seconds;
        else
            utc_seconds -= offset_seconds;

        const auto scale_multiplier = DecimalUtils::scaleMultiplier<Int128>(timestamp_scale);
        Int128 fractional = 0;
        const size_t fraction_digits = fraction_end - fraction_start;
        const size_t retained_fraction_digits = std::min({fraction_digits, size_t(9), static_cast<size_t>(timestamp_scale)});
        for (size_t i = 0; i < retained_fraction_digits; ++i)
            fractional = fractional * 10 + (input[fraction_start + i] - '0');
        if (retained_fraction_digits < timestamp_scale)
            fractional *= DecimalUtils::scaleMultiplier<Int128>(timestamp_scale - static_cast<UInt32>(retained_fraction_digits));

        result.value = utc_seconds * scale_multiplier + fractional;
        return true;
    }

    template <typename T>
    std::string_view getTypeName()
    {
        if constexpr (std::is_same_v<T, TimestampType>)
            return "timestamp";
        else if constexpr (std::is_same_v<T, RequestTimestampType>)
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
            if (equalsCaseInsensitive(input, "Inf") || equalsCaseInsensitive(input, "NaN"))
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
        Int64 value = 0;
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
        size_t last_unit_order = 0;

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
            size_t unit_order = 0;

            if (unit_name == "y")
            {
                unit_order = 1;
                seconds_per_unit = 365ULL * 24 * 60 * 60;  /// 1y equals 365d (ignoring leap days)
            }
            else if (unit_name == "w")
            {
                unit_order = 2;
                seconds_per_unit = 7 * 24 * 60 * 60;  /// 1w equals 7d
            }
            else if (unit_name == "d")
            {
                unit_order = 3;
                seconds_per_unit = 24 * 60 * 60;  /// 1d equals 24h
            }
            else if (unit_name == "h")
            {
                unit_order = 4;
                seconds_per_unit = 60 * 60;  /// 1h equals 60m
            }
            else if (unit_name == "m")
            {
                unit_order = 5;
                seconds_per_unit = 60;  /// 1m equals 60s
            }
            else if (unit_name == "s")
            {
                unit_order = 6;
                seconds_per_unit = 1;  /// 1s equals 1000ms
            }
            else if (unit_name == "ms")
            {
                unit_order = 7;
                ms_per_unit = 1;
            }
            else
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {} in duration format: Expected one of the supported time units ('y', 'w', 'd', 'h', 'm', 's', 'ms'), got {}",
                                getTypeName<T>(), quoteString(input), quoteString(unit_name));
                setErrorPos(error_pos, unit_start_pos);
                return false;
            }

            if (unit_order <= last_unit_order)
            {
                setErrorMessage(error_message,
                                "Cannot parse {} {} in duration format: Time units must be ordered from longest to shortest and must not be repeated",
                                getTypeName<T>(), quoteString(input));
                setErrorPos(error_pos, unit_start_pos);
                return false;
            }
            last_unit_order = unit_order;

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
            Int64 scaled_milliseconds = 0;
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

bool PrometheusQueryParsingUtil::tryParsePrometheusRequestTimestamp(
    std::string_view input,
    UInt32 timestamp_scale,
    RequestTimestampType & res_timestamp,
    String * error_message,
    size_t * error_pos)
{
    if (isPrometheusRequestTimestampNumber(input)
        && tryParsePrometheusRequestTimestampNumber(input, timestamp_scale, res_timestamp))
        return true;

    if (tryParsePrometheusRFC3339Timestamp(input, timestamp_scale, res_timestamp))
        return true;

    setErrorMessage(error_message, "Cannot parse Prometheus HTTP API timestamp {}", quoteString(input));
    setErrorPos(error_pos, 0);
    return false;
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
