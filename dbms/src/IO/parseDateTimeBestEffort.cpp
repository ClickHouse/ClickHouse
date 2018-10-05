#include <common/DateLUTImpl.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/parseDateTimeBestEffort.h>


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


template <typename ReturnType>
ReturnType parseDateTimeBestEffortImpl(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    auto on_error = [](const std::string & message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw Exception(message, code);
        else
            return false;
    };

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

    bool is_pm = false;

    while (!in.eof())
    {
        char digits[14];

        size_t num_digits = 0;

        if (!year || !has_time)
        {
            num_digits = readDigits(digits, sizeof(digits), in);

            if (num_digits == 10 && !year && !has_time)
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
                /// This is YYYYMM
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
                /// DD

                UInt8 hour_or_day_of_month = 0;
                if (num_digits == 2)
                    readDecimalNumber<2>(hour_or_day_of_month, digits);
                else if (num_digits == 1)   //-V547
                    readDecimalNumber<1>(hour_or_day_of_month, digits);
                else
                    return on_error("Cannot read DateTime: logical error, unexpected branch in code", ErrorCodes::LOGICAL_ERROR);

                if (checkChar(':', in))
                {
                    if (has_time)
                        return on_error("Cannot read DateTime: time component is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    hour = hour_or_day_of_month;
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
                else if (checkChar('/', in))
                {
                    if (day_of_month)
                        return on_error("Cannot read DateTime: day of month is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    if (month)
                        return on_error("Cannot read DateTime: month is duplicated", ErrorCodes::CANNOT_PARSE_DATETIME);

                    day_of_month = hour_or_day_of_month;

                    num_digits = readDigits(digits, sizeof(digits), in);

                    if (num_digits == 2)
                        readDecimalNumber<2>(month, digits);
                    else if (num_digits == 1)
                        readDecimalNumber<1>(month, digits);
                    else
                        return on_error("Cannot read DateTime: unexpected number of decimal digits after day of month: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);

                    if (checkChar('/', in))
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
                        hour = hour_or_day_of_month;
                    else
                        day_of_month = hour_or_day_of_month;
                }
            }
            else if (num_digits != 0)
                return on_error("Cannot read DateTime: unexpected number of decimal digits: " + toString(num_digits), ErrorCodes::CANNOT_PARSE_DATETIME);
        }

        if (num_digits == 0)
        {
            char c = *in.position();

            if (c == ' ' || c == 'T')
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

                /// Just ignore fractional part of second.
                readDigits(digits, sizeof(digits), in);
            }
            else if (c == '+' || c == '-')
            {
                ++in.position();
                has_time_zone_offset = true;
                if (c == '-')
                    time_zone_offset_negative = true;

                num_digits = readDigits(digits, sizeof(digits), in);

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

                    else if (0 == strncasecmp(alpha, "UTC", 3)) has_time_zone_offset = true;
                    else if (0 == strncasecmp(alpha, "GMT", 3)) has_time_zone_offset = true;
                    else if (0 == strncasecmp(alpha, "MSK", 3)) { has_time_zone_offset = true; time_zone_offset_hour = 3; }
                    else if (0 == strncasecmp(alpha, "MSD", 3)) { has_time_zone_offset = true; time_zone_offset_hour = 4; }

                    else if (0 == strncasecmp(alpha, "Mon", 3)) has_day_of_week = true;
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

    if (!year)
        year = 2000;
    if (!month)
        month = 1;
    if (!day_of_month)
        day_of_month = 1;

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

}


void parseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    parseDateTimeBestEffortImpl<void>(res, in, local_time_zone, utc_time_zone);
}

bool tryParseDateTimeBestEffort(time_t & res, ReadBuffer & in, const DateLUTImpl & local_time_zone, const DateLUTImpl & utc_time_zone)
{
    return parseDateTimeBestEffortImpl<bool>(res, in, local_time_zone, utc_time_zone);
}

}
