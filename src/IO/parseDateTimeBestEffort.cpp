#include <Common/DateLUTImpl.h>
#include <Common/StringUtils.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_DATETIME;
}


namespace
{

inline size_t readDigits(char * res, size_t max_chars, ReadBuffer & in)
{
    size_t num_chars = 0;
    while (!in.eof() && isNumericASCII(*in.position()) && num_chars < max_chars)
    {
        res[num_chars] = *in.position() - '0';
        ++num_chars;
        ++in.position();
    }
    return num_chars;
}

inline size_t readAlpha(char * res, size_t max_chars, ReadBuffer & in)
{
    size_t num_chars = 0;
    while (!in.eof() && isAlphaASCII(*in.position()) && num_chars < max_chars)
    {
        res[num_chars] = *in.position();
        ++num_chars;
        ++in.position();
    }
    return num_chars;
}

template <size_t digit, size_t power_of_ten, typename T>
inline void readDecimalNumberImpl(T & res, const char * src)
{
    res += src[digit] * power_of_ten;
    if constexpr (digit > 0)
        readDecimalNumberImpl<digit - 1, power_of_ten * 10>(res, src);
}

template <size_t num_digits, typename T>
inline void readDecimalNumber(T & res, const char * src)
{
    readDecimalNumberImpl<num_digits - 1, 1>(res, src);
}

template <typename T>
inline void readDecimalNumber(T & res, size_t num_digits, const char * src)
{
#define READ_DECIMAL_NUMBER(N) do { res *= common::exp10_i32(N); readDecimalNumber<N>(res, src); src += (N); num_digits -= (N); } while (false)
    while (num_digits)
    {
        switch (num_digits)
        {
            case 3: READ_DECIMAL_NUMBER(3); break;
            case 2: READ_DECIMAL_NUMBER(2); break;
            case 1: READ_DECIMAL_NUMBER(1); break;
            default: READ_DECIMAL_NUMBER(4); break;
        }
    }
#undef READ_DECIMAL_NUMBER
}

struct DateTimeSubsecondPart
{
    Int64 value;
    UInt8 digits;
};

template <typename ReturnType, bool is_us_style, bool strict = false, bool is_64 = false>
ReturnType parseDateTimeBestEffortImpl(
    time_t & res,
    ReadBuffer & in,
    const DateLUTImpl & local_time_zone,
    const DateLUTImpl & utc_time_zone,
    DateTimeSubsecondPart * fractional,
    const char * allowed_date_delimiters = nullptr)
{
    auto on_error = [&]<typename... FmtArgs>(int error_code [[maybe_unused]],
                                             FormatStringHelper<FmtArgs...> fmt_string [[maybe_unused]],
                                             FmtArgs && ...fmt_args [[maybe_unused]])
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw Exception(error_code, std::move(fmt_string), std::forward<FmtArgs>(fmt_args)...);
        else
            return false;
    };

    res = 0;
    UInt16 year = 0;
    UInt8 month = 0;
    UInt8 day_of_month = 0;
    UInt8 hour = 0;
    UInt8 minute = 0;
    UInt8 second = 0;

    bool has_time = false;

    bool has_time_zone_offset = false;
    bool time_zone_offset_negative = false;
    UInt8 time_zone_offset_hour = 0;
    UInt8 time_zone_offset_minute = 0;

    bool is_am = false;
    bool is_pm = false;

    bool has_comma_between_date_and_time = false;

    auto read_alpha_month = [&month] (const auto & alpha)
    {
        if (0 == strncasecmp(alpha, "Jan", 3)) month = 1;
        else if (0 == strncasecmp(alpha, "Feb", 3)) month = 2;
        else if (0 == strncasecmp(alpha, "Mar", 3)) month = 3;
        else if (0 == strncasecmp(alpha, "Apr", 3)) month = 4;
        else if (0 == strncasecmp(alpha, "May", 3)) month = 5;
        else if (0 == strncasecmp(alpha, "Jun", 3)) month = 6;
        else if (0 == strncasecmp(alpha, "Jul", 3)) month = 7;
        else if (0 == strncasecmp(alpha, "Aug", 3)) month = 8;
        else if (0 == strncasecmp(alpha, "Sep", 3)) month = 9;
        else if (0 == strncasecmp(alpha, "Oct", 3)) month = 10;
        else if (0 == strncasecmp(alpha, "Nov", 3)) month = 11;
        else if (0 == strncasecmp(alpha, "Dec", 3)) month = 12;
        else
            return false;
        return true;
    };

    while (!in.eof())
    {
        if ((year && !has_time) || (!year && has_time))
        {
            if (*in.position() == ',')
            {
                has_comma_between_date_and_time = true;
                ++in.position();

                if (in.eof())
                    break;
            }
        }

        char digits[std::numeric_limits<UInt64>::digits10];

        size_t num_digits = 0;

        if (!year || !has_time)
        {
            num_digits = readDigits(digits, sizeof(digits), in);

            if (num_digits == 13 && !year && !has_time)
            {
                /// This is unix timestamp with millisecond.
                readDecimalNumber<10>(res, digits);
                if (fractional)
                {
                    fractional->digits = 3;
                    readDecimalNumber<3>(fractional->value, digits + 10);
                }
                else if constexpr (strict)
                {
                    /// Fractional part is not allowed.
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected fractional part");
                }
                return ReturnType(true);
            }
            if (num_digits == 10 && !year && !has_time)
            {
                if (strict)
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Strict best effort parsing doesn't allow timestamps");

                /// This is unix timestamp.
                readDecimalNumber<10>(res, digits);
                return ReturnType(true);
            }
            if (num_digits == 9 && !year && !has_time)
            {
                if (strict)
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Strict best effort parsing doesn't allow timestamps");

                /// This is unix timestamp.
                readDecimalNumber<9>(res, digits);
                return ReturnType(true);
            }
            if (num_digits == 14 && !year && !has_time)
            {
                if (strict)
                    return on_error(
                        ErrorCodes::CANNOT_PARSE_DATETIME, "Strict best effort parsing doesn't allow date times without separators");

                /// This is YYYYMMDDhhmmss
                readDecimalNumber<4>(year, digits);
                readDecimalNumber<2>(month, digits + 4);
                readDecimalNumber<2>(day_of_month, digits + 6);
                readDecimalNumber<2>(hour, digits + 8);
                readDecimalNumber<2>(minute, digits + 10);
                readDecimalNumber<2>(second, digits + 12);
                has_time = true;
            }
            else if (num_digits == 8 && !year)
            {
                if (strict)
                    return on_error(
                        ErrorCodes::CANNOT_PARSE_DATETIME, "Strict best effort parsing doesn't allow date times without separators");

                /// This is YYYYMMDD
                readDecimalNumber<4>(year, digits);
                readDecimalNumber<2>(month, digits + 4);
                readDecimalNumber<2>(day_of_month, digits + 6);
            }
            else if (num_digits == 6)
            {
                if (strict)
                    return on_error(
                        ErrorCodes::CANNOT_PARSE_DATETIME, "Strict best effort parsing doesn't allow date times without separators");

                /// This is YYYYMM or hhmmss
                if (!year && !month)
                {
                    readDecimalNumber<4>(year, digits);
                    readDecimalNumber<2>(month, digits + 4);
                }
                else if (!has_time)
                {
                    readDecimalNumber<2>(hour, digits);
                    readDecimalNumber<2>(minute, digits + 2);
                    readDecimalNumber<2>(second, digits + 4);
                    has_time = true;
                }
                else
                    return on_error(
                        ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: ambiguous 6 digits, it can be YYYYMM or hhmmss");
            }
            else if (num_digits == 4 && !year)
            {
                /// YYYY
                /// YYYY*MM
                /// YYYY*MM*DD
                /// YYYY*M
                /// YYYY*M*DD
                /// YYYY*M*D

                readDecimalNumber<4>(year, digits);

                if (!in.eof())
                {
                    char delimiter_after_year = *in.position();

                    if (delimiter_after_year < 0x20 || delimiter_after_year == ',' || delimiter_after_year == ';'
                        || delimiter_after_year == '\'' || delimiter_after_year == '"')
                        break;

                    if (month)
                        continue;

                    ++in.position();

                    num_digits = readDigits(digits, sizeof(digits), in);

                    if (num_digits == 2)
                        readDecimalNumber<2>(month, digits);
                    else if (num_digits == 1)
                        readDecimalNumber<1>(month, digits);
                    else if (delimiter_after_year == ' ')
                        continue;
                    else
                        return on_error(
                            ErrorCodes::CANNOT_PARSE_DATETIME,
                            "Cannot read DateTime: unexpected number of decimal digits after year: {}",
                            num_digits);

                    /// Only the same delimiter.
                    if (!day_of_month && checkChar(delimiter_after_year, in))
                    {
                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 2)
                            readDecimalNumber<2>(day_of_month, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(day_of_month, digits);
                        else if (delimiter_after_year == ' ')
                            continue;
                        else
                            return on_error(
                                ErrorCodes::CANNOT_PARSE_DATETIME,
                                "Cannot read DateTime: unexpected number of decimal digits after year and month: {}",
                                num_digits);
                    }

                    if (!isSymbolIn(delimiter_after_year, allowed_date_delimiters))
                        return on_error(
                            ErrorCodes::CANNOT_PARSE_DATETIME,
                            "Cannot read DateTime: '{}' delimiter between date parts is not allowed",
                            delimiter_after_year);
                }
            }
            else if (num_digits == 2 || num_digits == 1)
            {
                /// hh:mm:ss
                /// hh:mm
                /// hh - only if already have day of month
                /// DD/MM/YYYY
                /// DD/MM/YY
                /// DD.MM.YYYY
                /// DD.MM.YY
                /// DD-MM-YYYY
                /// DD-MM-YY
                /// DD

                UInt8 hour_or_day_of_month_or_month = 0;
                if (num_digits == 2)
                    readDecimalNumber<2>(hour_or_day_of_month_or_month, digits);
                else if (num_digits == 1)
                    readDecimalNumber<1>(hour_or_day_of_month_or_month, digits);
                else
                    return on_error(ErrorCodes::LOGICAL_ERROR, "Cannot read DateTime: logical error, unexpected branch in code");

                if (checkChar(':', in))
                {
                    if (has_time)
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: time component is duplicated");

                    hour = hour_or_day_of_month_or_month;
                    has_time = true;

                    num_digits = readDigits(digits, sizeof(digits), in);

                    if (num_digits == 2)
                        readDecimalNumber<2>(minute, digits);
                    else if (num_digits == 1)
                        readDecimalNumber<1>(minute, digits);
                    else
                        return on_error(
                            ErrorCodes::CANNOT_PARSE_DATETIME,
                            "Cannot read DateTime: unexpected number of decimal digits after hour: {}",
                            num_digits);

                    if (checkChar(':', in))
                    {
                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 2)
                            readDecimalNumber<2>(second, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(second, digits);
                        else
                            return on_error(
                                ErrorCodes::CANNOT_PARSE_DATETIME,
                                "Cannot read DateTime: unexpected number of decimal digits after hour and minute: {}",
                                num_digits);
                    }
                }
                else if (checkChar(',', in))
                {
                    if (month && !day_of_month)
                        day_of_month = hour_or_day_of_month_or_month;
                }
                else if (
                    (!in.eof() && isSymbolIn(*in.position(), allowed_date_delimiters))
                    && (checkChar('/', in) || checkChar('.', in) || checkChar('-', in)))
                {
                    if (day_of_month)
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: day of month is duplicated");

                    if (month)
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: month is duplicated");

                    if constexpr (is_us_style)
                    {
                        month = hour_or_day_of_month_or_month;
                        num_digits = readDigits(digits, sizeof(digits), in);
                        if (num_digits == 2)
                            readDecimalNumber<2>(day_of_month, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(day_of_month, digits);
                        else
                            return on_error(
                                ErrorCodes::CANNOT_PARSE_DATETIME,
                                "Cannot read DateTime: unexpected number of decimal digits after month: {}",
                                num_digits);
                    }
                    else
                    {
                        day_of_month = hour_or_day_of_month_or_month;

                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 2)
                            readDecimalNumber<2>(month, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(month, digits);
                        else if (num_digits == 0)
                        {
                            /// Month in alphabetical form

                            char alpha[9]; /// The longest month name: September
                            size_t num_alpha = readAlpha(alpha, sizeof(alpha), in);

                            if (num_alpha < 3)
                                return on_error(
                                    ErrorCodes::CANNOT_PARSE_DATETIME,
                                    "Cannot read DateTime: unexpected number of alphabetical characters after day of month: {}",
                                    num_alpha);

                            if (!read_alpha_month(alpha))
                                return on_error(
                                    ErrorCodes::CANNOT_PARSE_DATETIME,
                                    "Cannot read DateTime: alphabetical characters after day of month don't look like month: {}",
                                    std::string(alpha, 3));
                        }
                        else
                            return on_error(
                                ErrorCodes::CANNOT_PARSE_DATETIME,
                                "Cannot read DateTime: unexpected number of decimal digits after day of month: {}",
                                num_digits);
                    }

                    if (month > 12)
                        std::swap(month, day_of_month);

                    if ((!in.eof() && isSymbolIn(*in.position(), allowed_date_delimiters))
                        && (checkChar('/', in) || checkChar('.', in) || checkChar('-', in)))
                    {
                        if (year)
                            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: year component is duplicated");

                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 4)
                            readDecimalNumber<4>(year, digits);
                        else if (num_digits == 2)
                        {
                            readDecimalNumber<2>(year, digits);

                            if (year >= 70)
                                year += 1900;
                            else
                                year += 2000;
                        }
                        else
                            return on_error(
                                ErrorCodes::CANNOT_PARSE_DATETIME,
                                "Cannot read DateTime: unexpected number of decimal digits after day of month and month: {}",
                                num_digits);
                    }
                }
                else
                {
                    if (day_of_month)
                    {
                        if (strict && hour)
                            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: hour component is duplicated");

                        hour = hour_or_day_of_month_or_month;
                    }
                    else
                    {
                        day_of_month = hour_or_day_of_month_or_month;
                    }
                }
            }
            else if (num_digits != 0)
                return on_error(
                    ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected number of decimal digits: {}", num_digits);
        }

        if (num_digits == 0)
        {
            char c = *in.position();

            /// 'T' is a separator between date and time according to ISO 8601.
            /// But don't skip it if we didn't read the date part yet, because 'T' is also a prefix for 'Tue' and 'Thu'.

            if (c == ' ' || (c == 'T' && year && !has_time))
            {
                ++in.position();
            }
            else if (c == 'Z')
            {
                ++in.position();
                has_time_zone_offset = true;
            }
            else if (c == '.')  /// We don't support comma (ISO 8601:2004) for fractional part of second to not mess up with CSV separator.
            {
                if (!has_time)
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected point symbol");

                ++in.position();
                num_digits = readDigits(digits, sizeof(digits), in);
                if (fractional)
                {
                    using FractionalType = typename std::decay_t<decltype(fractional->value)>;
                    // Reading more decimal digits than fits into FractionalType would case an
                    // overflow, so it is better to skip all digits from the right side that do not
                    // fit into result type. To provide less precise value rather than bogus one.
                    num_digits = std::min(static_cast<size_t>(std::numeric_limits<FractionalType>::digits10), num_digits);

                    fractional->digits = num_digits;
                    readDecimalNumber(fractional->value, num_digits, digits);
                }
                else if (strict)
                {
                    /// Fractional part is not allowed.
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected fractional part");
                }
            }
            else if (c == '+' || c == '-')
            {
                ++in.position();
                num_digits = readDigits(digits, sizeof(digits), in);

                if (num_digits == 6 && !has_time && year && month && day_of_month)
                {
                    /// It looks like hhmmss
                    readDecimalNumber<2>(hour, digits);
                    readDecimalNumber<2>(minute, digits + 2);
                    readDecimalNumber<2>(second, digits + 4);
                    has_time = true;
                }
                else
                {
                    /// It looks like time zone offset
                    has_time_zone_offset = true;
                    if (c == '-')
                        time_zone_offset_negative = true;

                    if (num_digits == 4)
                    {
                        readDecimalNumber<2>(time_zone_offset_hour, digits);
                        readDecimalNumber<2>(time_zone_offset_minute, digits + 2);
                    }
                    else if (num_digits == 3)
                    {
                        readDecimalNumber<1>(time_zone_offset_hour, digits);
                        readDecimalNumber<2>(time_zone_offset_minute, digits + 1);
                    }
                    else if (num_digits == 2)
                    {
                        readDecimalNumber<2>(time_zone_offset_hour, digits);
                    }
                    else if (num_digits == 1)
                    {
                        readDecimalNumber<1>(time_zone_offset_hour, digits);
                    }
                    else
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected number of decimal digits for time zone offset: {}", num_digits);

                    if (num_digits < 3 && checkChar(':', in))
                    {
                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 2)
                        {
                            readDecimalNumber<2>(time_zone_offset_minute, digits);
                        }
                        else if (num_digits == 1)
                        {
                            readDecimalNumber<1>(time_zone_offset_minute, digits);
                        }
                        else
                            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected number of decimal digits for time zone offset in minutes: {}", num_digits);
                    }
                }
            }
            else
            {
                char alpha[3];

                size_t num_alpha = readAlpha(alpha, sizeof(alpha), in);

                if (!num_alpha)
                {
                    break;
                }
                if (num_alpha == 1)
                {
                    return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected alphabetical character");
                }
                if (num_alpha == 2)
                {
                    if (alpha[1] == 'M' || alpha[1] == 'm')
                    {
                        if (alpha[0] == 'A' || alpha[0] == 'a')
                        {
                            is_am = true;
                        }
                        else if (alpha[0] == 'P' || alpha[0] == 'p')
                        {
                            is_pm = true;
                        }
                        else
                            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected word");
                    }
                    else
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected word");
                }
                else if (num_alpha == 3)
                {
                    bool has_day_of_week = false;

                    if (read_alpha_month(alpha))
                    {
                    }
                    else if (0 == strncasecmp(alpha, "UTC", 3))
                        has_time_zone_offset = true; // NOLINT
                    else if (0 == strncasecmp(alpha, "GMT", 3))
                        has_time_zone_offset = true;
                    else if (0 == strncasecmp(alpha, "MSK", 3))
                    {
                        has_time_zone_offset = true;
                        time_zone_offset_hour = 3;
                    }
                    else if (0 == strncasecmp(alpha, "MSD", 3))
                    {
                        has_time_zone_offset = true;
                        time_zone_offset_hour = 4;
                    }

                    else if (0 == strncasecmp(alpha, "Mon", 3))
                        has_day_of_week = true; // NOLINT
                    else if (0 == strncasecmp(alpha, "Tue", 3))
                        has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Wed", 3))
                        has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Thu", 3))
                        has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Fri", 3))
                        has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Sat", 3))
                        has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Sun", 3))
                        has_day_of_week = true;

                    else
                        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected word");

                    while (!in.eof() && isAlphaASCII(*in.position()))
                        ++in.position();

                    /// For RFC 2822
                    if (has_day_of_week)
                        checkChar(',', in);
                }
                else
                    return on_error(ErrorCodes::LOGICAL_ERROR, "Cannot read DateTime: logical error, unexpected branch in code");
            }
        }
    }

    //// Date like '2022/03/04, ' should parse fail?
    if (has_comma_between_date_and_time && (!has_time || !year || !month || !day_of_month))
        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected word after Date");

    /// If neither Date nor Time is parsed successfully, it should fail
    if (!year && !month && !day_of_month && !has_time)
        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: neither Date nor Time was parsed successfully");

    if (!day_of_month)
    {
        if constexpr (strict)
            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: day of month is required");
        day_of_month = 1;
    }

    if (!month)
    {
        if constexpr (strict)
            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: month is required");
        month = 1;
    }

    if (!year)
    {
        if constexpr (strict)
            return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: year is required");

        /// If year is not specified, it will be the current year if the date is unknown or not greater than today,
        /// otherwise it will be the previous year.
        /// This convoluted logic is needed to parse the syslog format, which looks as follows: "Mar  3 01:33:48".
        /// If you have questions, ask Victor Krasnov, https://www.linkedin.com/in/vickr/

        time_t now = time(nullptr);
        auto today = local_time_zone.toDayNum(now);
        UInt16 curr_year = local_time_zone.toYear(today);
        year = local_time_zone.makeDayNum(curr_year, month, day_of_month) <= today ? curr_year : curr_year - 1;
    }

    auto is_leap_year = (year % 400 == 0) || (year % 100 != 0 && year % 4 == 0);

    auto check_date = [](const auto & is_leap_year_, const auto & month_, const auto & day_)
    {
        if ((month_ == 1 || month_ == 3 || month_ == 5 || month_ == 7 || month_ == 8 || month_ == 10 || month_ == 12) && day_ >= 1 && day_ <= 31)
            return true;
        if (month_ == 2 && ((is_leap_year_ && day_ >= 1 && day_ <= 29) || (!is_leap_year_ && day_ >= 1 && day_ <= 28)))
            return true;
        if ((month_ == 4 || month_ == 6 || month_ == 9 || month_ == 11) && day_ >= 1 && day_ <= 30)
            return true;
        return false;
    };

    if (!check_date(is_leap_year, month, day_of_month))
        return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: unexpected date: {}-{}-{}",
                        year, static_cast<UInt16>(month), static_cast<UInt16>(day_of_month));

    if (is_am && hour == 12)
        hour = 0;

    if (is_pm && hour < 12)
        hour += 12;

    auto adjust_time_zone = [&]
    {
        if (time_zone_offset_hour)
        {
            if (time_zone_offset_negative)
                res += time_zone_offset_hour * 3600;
            else
                res -= time_zone_offset_hour * 3600;
        }

        if (time_zone_offset_minute)
        {
            if (time_zone_offset_negative)
                res += time_zone_offset_minute * 60;
            else
                res -= time_zone_offset_minute * 60;
        }
    };

    if constexpr (strict)
    {
        if constexpr (is_64)
        {
            if (year < 1900)
                return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime64: year {} is less than minimum supported year 1900", year);
        }
        else
        {
            if (year < 1970)
                return on_error(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot read DateTime: year {} is less than minimum supported year 1970", year);
        }
    }

    if (has_time_zone_offset)
    {
        res = utc_time_zone.makeDateTime(year, month, day_of_month, hour, minute, second);
        adjust_time_zone();
    }
    else
    {
        res = local_time_zone.makeDateTime(year, month, day_of_month, hour, minute, second);
    }

    return ReturnType(true);
}

template <typename ReturnType, bool is_us_style, bool strict = false>
ReturnType parseDateTime64BestEffortImpl(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters = nullptr)
{
    time_t whole;
    DateTimeSubsecondPart subsecond = {0, 0}; // needs to be explicitly initialized sine it could be missing from input string

    if constexpr (std::is_same_v<ReturnType, bool>)
    {
        if (!parseDateTimeBestEffortImpl<bool, is_us_style, strict, true>(whole, in, local_time_zone, utc_time_zone, &subsecond, allowed_date_delimiters))
            return false;
    }
    else
    {
        parseDateTimeBestEffortImpl<ReturnType, is_us_style, strict, true>(whole, in, local_time_zone, utc_time_zone, &subsecond, allowed_date_delimiters);
    }


    DateTime64::NativeType fractional = subsecond.value;
    if (scale < subsecond.digits)
    {
        fractional /= common::exp10_i64(subsecond.digits - scale);
    }
    else if (scale > subsecond.digits)
    {
        fractional *= common::exp10_i64(scale - subsecond.digits);
    }

    if constexpr (std::is_same_v<ReturnType, bool>)
        return DecimalUtils::tryGetDecimalFromComponents<DateTime64>(whole, fractional, scale, res);

    res = DecimalUtils::decimalFromComponents<DateTime64>(whole, fractional, scale);
    return ReturnType(true);
}

}

void parseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    parseDateTimeBestEffortImpl<void, false>(res, in, local_time_zone, utc_time_zone, nullptr);
}

void parseDateTimeBestEffortUS(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    parseDateTimeBestEffortImpl<void, true>(res, in, local_time_zone, utc_time_zone, nullptr);
}

bool tryParseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTimeBestEffortImpl<bool, false>(res, in, local_time_zone, utc_time_zone, nullptr);
}

bool tryParseDateTimeBestEffortUS(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTimeBestEffortImpl<bool, true>(res, in, local_time_zone, utc_time_zone, nullptr);
}

void parseDateTime64BestEffort(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    parseDateTime64BestEffortImpl<void, false>(res, scale, in, local_time_zone, utc_time_zone);
}

void parseDateTime64BestEffortUS(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    parseDateTime64BestEffortImpl<void, true>(res, scale, in, local_time_zone, utc_time_zone);
}

bool tryParseDateTime64BestEffort(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTime64BestEffortImpl<bool, false>(res, scale, in, local_time_zone, utc_time_zone);
}

bool tryParseDateTime64BestEffortUS(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTime64BestEffortImpl<bool, true>(res, scale, in, local_time_zone, utc_time_zone);
}

bool tryParseDateTimeBestEffortStrict(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters)
{
    return parseDateTimeBestEffortImpl<bool, false, true>(res, in, local_time_zone, utc_time_zone, nullptr, allowed_date_delimiters);
}

bool tryParseDateTimeBestEffortUSStrict(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters)
{
    return parseDateTimeBestEffortImpl<bool, true, true>(res, in, local_time_zone, utc_time_zone, nullptr, allowed_date_delimiters);
}

bool tryParseDateTime64BestEffortStrict(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters)
{
    return parseDateTime64BestEffortImpl<bool, false, true>(res, scale, in, local_time_zone, utc_time_zone, allowed_date_delimiters);
}

bool tryParseDateTime64BestEffortUSStrict(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone, const char * allowed_date_delimiters)
{
    return parseDateTime64BestEffortImpl<bool, true, true>(res, scale, in, local_time_zone, utc_time_zone, allowed_date_delimiters);
}

}
