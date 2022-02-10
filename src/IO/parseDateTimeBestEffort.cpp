#include <Common/DateLUTImpl.h>
#include <Common/StringUtils/StringUtils.h>

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

#if defined(__PPC__)
#if !defined(__clang__)
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#endif

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

template <typename ReturnType, bool is_us_style>
ReturnType parseDateTimeBestEffortImpl(
    time_t & res,
    ReadBuffer & in,
    const DateLUTImpl & local_time_zone,
    const DateLUTImpl & utc_time_zone,
    DateTimeSubsecondPart * fractional)
{
    auto on_error = [](const std::string & message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw ParsingException(message, code);
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
                return ReturnType(true);
            }
            else if (num_digits == 10 && !year && !has_time)
            {
                /// This is unix timestamp.
                readDecimalNumber<10>(res, digits);
                return ReturnType(true);
            }
            else if (num_digits == 9 && !year && !has_time)
            {
                /// This is unix timestamp.
                readDecimalNumber<9>(res, digits);
                return ReturnType(true);
            }
            else if (num_digits == 14 && !year && !has_time)
            {
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
                /// This is YYYYMMDD
                readDecimalNumber<4>(year, digits);
                readDecimalNumber<2>(month, digits + 4);
                readDecimalNumber<2>(day_of_month, digits + 6);
            }
            else if (num_digits == 6)
            {
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
                    return on_error("Cannot read DateTime: ambiguous 6 digits, it can be YYYYMM or hhmmss", ErrorCodes::CANNOT_PARSE_DATETIME);
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

                    if (delimiter_after_year < 0x20
                        || delimiter_after_year == ','
                        || delimiter_after_year == ';'
                        || delimiter_after_year == '\''
                        || delimiter_after_year == '"')
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
                        return on_error("Cannot read DateTime: unexpected number of decimal digits after year: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);

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
                            return on_error("Cannot read DateTime: unexpected number of decimal digits after year and month: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
                    }
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
                else if (num_digits == 1)   //-V547
                    readDecimalNumber<1>(hour_or_day_of_month_or_month, digits);
                else
                    return on_error("Cannot read DateTime: logical error, unexpected branch in code", ErrorCodes::LOGICAL_ERROR);

                if (checkChar(':', in))
                {
                    if (has_time)
                        return on_error("Cannot read DateTime: time component is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    hour = hour_or_day_of_month_or_month;
                    has_time = true;

                    num_digits = readDigits(digits, sizeof(digits), in);

                    if (num_digits == 2)
                        readDecimalNumber<2>(minute, digits);
                    else if (num_digits == 1)
                        readDecimalNumber<1>(minute, digits);
                    else
                        return on_error("Cannot read DateTime: unexpected number of decimal digits after hour: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);

                    if (checkChar(':', in))
                    {
                        num_digits = readDigits(digits, sizeof(digits), in);

                        if (num_digits == 2)
                            readDecimalNumber<2>(second, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(second, digits);
                        else
                            return on_error("Cannot read DateTime: unexpected number of decimal digits after hour and minute: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
                    }
                }
                else if (checkChar('/', in) || checkChar('.', in) || checkChar('-', in))
                {
                    if (day_of_month)
                        return on_error("Cannot read DateTime: day of month is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    if (month)
                        return on_error("Cannot read DateTime: month is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    if constexpr (is_us_style)
                    {
                        month = hour_or_day_of_month_or_month;
                        num_digits = readDigits(digits, sizeof(digits), in);
                        if (num_digits == 2)
                            readDecimalNumber<2>(day_of_month, digits);
                        else if (num_digits == 1)
                            readDecimalNumber<1>(day_of_month, digits);
                        else
                            return on_error("Cannot read DateTime: unexpected number of decimal digits after month: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
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

                          char alpha[9];  /// The longest month name: September
                          size_t num_alpha = readAlpha(alpha, sizeof(alpha), in);

                          if (num_alpha < 3)
                              return on_error("Cannot read DateTime: unexpected number of alphabetical characters after day of month: " + toString(num_alpha), ErrorCodes::CANNOT_PARSE_DATETIME);

                          if (!read_alpha_month(alpha))
                              return on_error("Cannot read DateTime: alphabetical characters after day of month don't look like month: " + std::string(alpha, 3), ErrorCodes::CANNOT_PARSE_DATETIME);
                        }
                        else
                          return on_error("Cannot read DateTime: unexpected number of decimal digits after day of month: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
                    }

                    if (month > 12)
                        std::swap(month, day_of_month);

                    if (checkChar('/', in) || checkChar('.', in) || checkChar('-', in))
                    {
                        if (year)
                            return on_error("Cannot read DateTime: year component is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

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
                            return on_error("Cannot read DateTime: unexpected number of decimal digits after day of month and month: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
                    }
                }
                else
                {
                    if (day_of_month)
                        hour = hour_or_day_of_month_or_month;
                    else
                        day_of_month = hour_or_day_of_month_or_month;
                }
            }
            else if (num_digits != 0)
                return on_error("Cannot read DateTime: unexpected number of decimal digits: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
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
                    return on_error("Cannot read DateTime: unexpected point symbol", ErrorCodes::CANNOT_PARSE_DATETIME);

                ++in.position();
                num_digits = readDigits(digits, sizeof(digits), in);
                if (fractional)
                {
                    using FractionalType = typename std::decay<decltype(fractional->value)>::type;
                    // Reading more decimal digits than fits into FractionalType would case an
                    // overflow, so it is better to skip all digits from the right side that do not
                    // fit into result type. To provide less precise value rather than bogus one.
                    num_digits = std::min(static_cast<size_t>(std::numeric_limits<FractionalType>::digits10), num_digits);

                    fractional->digits = num_digits;
                    readDecimalNumber(fractional->value, num_digits, digits);
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
                        return on_error("Cannot read DateTime: unexpected number of decimal digits for time zone offset: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);

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
                            return on_error("Cannot read DateTime: unexpected number of decimal digits for time zone offset in minutes: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
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
                else if (num_alpha == 1)
                {
                    return on_error("Cannot read DateTime: unexpected alphabetical character", ErrorCodes::CANNOT_PARSE_DATETIME);
                }
                else if (num_alpha == 2)
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
                            return on_error("Cannot read DateTime: unexpected word", ErrorCodes::CANNOT_PARSE_DATETIME);
                    }
                    else
                        return on_error("Cannot read DateTime: unexpected word", ErrorCodes::CANNOT_PARSE_DATETIME);
                }
                else if (num_alpha == 3)
                {
                    bool has_day_of_week = false;

                    if (read_alpha_month(alpha))
                    {
                    }
                    else if (0 == strncasecmp(alpha, "UTC", 3)) has_time_zone_offset = true; // NOLINT
                    else if (0 == strncasecmp(alpha, "GMT", 3)) has_time_zone_offset = true;
                    else if (0 == strncasecmp(alpha, "MSK", 3)) { has_time_zone_offset = true; time_zone_offset_hour = 3; }
                    else if (0 == strncasecmp(alpha, "MSD", 3)) { has_time_zone_offset = true; time_zone_offset_hour = 4; }

                    else if (0 == strncasecmp(alpha, "Mon", 3)) has_day_of_week = true; // NOLINT
                    else if (0 == strncasecmp(alpha, "Tue", 3)) has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Wed", 3)) has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Thu", 3)) has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Fri", 3)) has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Sat", 3)) has_day_of_week = true;
                    else if (0 == strncasecmp(alpha, "Sun", 3)) has_day_of_week = true;

                    else
                        return on_error("Cannot read DateTime: unexpected word", ErrorCodes::CANNOT_PARSE_DATETIME);

                    while (!in.eof() && isAlphaASCII(*in.position()))
                        ++in.position();

                    /// For RFC 2822
                    if (has_day_of_week)
                        checkChar(',', in);
                }
                else
                    return on_error("Cannot read DateTime: logical error, unexpected branch in code", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    /// If neither Date nor Time is parsed successfully, it should fail
    if (!year && !month && !day_of_month && !has_time)
        return on_error("Cannot read DateTime: neither Date nor Time was parsed successfully", ErrorCodes::CANNOT_PARSE_DATETIME);

    if (!year)
        year = 2000;
    if (!month)
        month = 1;
    if (!day_of_month)
        day_of_month = 1;

    auto is_leap_year = (year % 400 == 0) || (year % 100 != 0 && year % 4 == 0);

    auto check_date = [](const auto & is_leap_year_, const auto & month_, const auto & day_)
    {
        if ((month_ == 1 || month_ == 3 || month_ == 5 || month_ == 7 || month_ == 8 || month_ == 10 || month_ == 12) && day_ >= 1 && day_ <= 31)
            return true;
        else if (month_ == 2 && ((is_leap_year_ && day_ >= 1 && day_ <= 29) || (!is_leap_year_ && day_ >= 1 && day_ <= 28)))
            return true;
        else if ((month_ == 4 || month_ == 6 || month_ == 9 || month_ == 11) && day_ >= 1 && day_ <= 30)
            return true;
        return false;
    };

    if (!check_date(is_leap_year, month, day_of_month))
        return on_error("Cannot read DateTime: unexpected date: " + std::to_string(year) + "-" + std::to_string(month) + "-" + std::to_string(day_of_month), ErrorCodes::CANNOT_PARSE_DATETIME);

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

template <typename ReturnType, bool is_us_style>
ReturnType parseDateTime64BestEffortImpl(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    time_t whole;
    DateTimeSubsecondPart subsecond = {0, 0}; // needs to be explicitly initialized sine it could be missing from input string

    if constexpr (std::is_same_v<ReturnType, bool>)
    {
        if (!parseDateTimeBestEffortImpl<bool, is_us_style>(whole, in, local_time_zone, utc_time_zone, &subsecond))
            return false;
    }
    else
    {
        parseDateTimeBestEffortImpl<ReturnType, is_us_style>(whole, in, local_time_zone, utc_time_zone, &subsecond);
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

    res = DecimalUtils::decimalFromComponents<DateTime64>(whole, fractional, scale);
    return ReturnType(true);
}

}

#if defined(__PPC__)
#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#endif

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
    return parseDateTime64BestEffortImpl<void, false>(res, scale, in, local_time_zone, utc_time_zone);
}

void parseDateTime64BestEffortUS(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTime64BestEffortImpl<void, true>(res, scale, in, local_time_zone, utc_time_zone);
}

bool tryParseDateTime64BestEffort(DateTime64 & res, UInt32 scale, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTime64BestEffortImpl<bool, false>(res, scale, in, local_time_zone, utc_time_zone);
}

}
