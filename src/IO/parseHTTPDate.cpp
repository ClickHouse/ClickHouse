#include <IO/parseHTTPDate.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <array>


namespace DB
{

namespace
{

constexpr std::array<std::string_view, 7> day_names{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};
constexpr std::array<std::string_view, 7> long_day_names{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
constexpr std::array<std::string_view, 12> month_names{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

constexpr std::string_view imf_fixdate_example{"Sun, 06 Nov 1994 08:49:37 GMT"};
constexpr std::string_view rfc850_date_rest_example{", 06-Nov-94 08:49:37 GMT"};
constexpr std::string_view asctime_date_example{"Sun Nov  6 08:49:37 1994"};

std::optional<UInt32> parseNumber(std::string_view date, size_t pos, size_t size)
{
    UInt32 res = 0;
    for (size_t i = pos; i < pos + size; ++i)
    {
        if (date[i] < '0' || date[i] > '9')
            return std::nullopt;
        res = res * 10 + (date[i] - '0');
    }
    return res;
}

bool isDayName(std::string_view name)
{
    return std::ranges::any_of(day_names, [&](std::string_view day_name) { return equalsCaseInsensitive(day_name, name); });
}

std::optional<UInt8> parseMonth(std::string_view name)
{
    const auto it = std::ranges::find_if(month_names, [&](std::string_view month_name) { return equalsCaseInsensitive(month_name, name); });
    if (it == month_names.end())
        return std::nullopt;
    return static_cast<UInt8>(it - month_names.begin() + 1);
}

struct TimeOfDay
{
    UInt8 hour;
    UInt8 minute;
    UInt8 second;
};

std::optional<TimeOfDay> parseTimeOfDay(std::string_view date, size_t pos)
{
    if (date[pos + 2] != ':' || date[pos + 5] != ':')
        return std::nullopt;

    std::optional<UInt32> hour = parseNumber(date, pos, 2);
    std::optional<UInt32> minute = parseNumber(date, pos + 3, 2);
    std::optional<UInt32> second = parseNumber(date, pos + 6, 2);

    if (!hour || !minute || !second)
        return std::nullopt;

    if (*hour > 23 || *minute > 59 || *second > 60)
        return std::nullopt;

    return TimeOfDay{static_cast<UInt8>(*hour), static_cast<UInt8>(*minute), static_cast<UInt8>(*second)};
}

std::optional<time_t> makeTimestamp(UInt32 year, UInt8 month, UInt32 day, TimeOfDay time)
{
    const DateLUTImpl & date_lut = DateLUT::instance("UTC");

    if (day < 1 || day > date_lut.daysInMonth(static_cast<Int16>(year), month))
        return std::nullopt;

    return date_lut.tryToMakeDateTime(
        static_cast<Int16>(year), month, static_cast<UInt8>(day), time.hour, time.minute, time.second);
}

std::optional<time_t> tryParseIMFFixdate(std::string_view date)
{
    if (date.size() != imf_fixdate_example.size())
        return std::nullopt;

    if (date.substr(3, 2) != ", " || date[7] != ' ' || date[11] != ' ' || date[16] != ' ' || date[25] != ' '
        || !equalsCaseInsensitive(date.substr(26), "GMT"))
        return std::nullopt;

    if (!isDayName(date.substr(0, 3)))
        return std::nullopt;

    std::optional<UInt8> month = parseMonth(date.substr(8, 3));
    std::optional<UInt32> day = parseNumber(date, 5, 2);
    std::optional<UInt32> year = parseNumber(date, 12, 4);
    std::optional<TimeOfDay> time = parseTimeOfDay(date, 17);

    if (!month || !day || !year || !time)
        return std::nullopt;

    return makeTimestamp(*year, *month, *day, *time);
}

std::optional<time_t> makeTimestampFromTwoDigitYear(
    UInt32 two_digit_year, UInt8 month, UInt32 day, TimeOfDay time, time_t reference_time)
{
    const DateLUTImpl & date_lut = DateLUT::instance("UTC");

    const UInt32 base_year = date_lut.toYear(static_cast<Int64>(reference_time)) / 100 * 100 + two_digit_year;
    const time_t window_begin = date_lut.addYears(static_cast<Int64>(reference_time), -50);
    const time_t window_end = date_lut.addYears(static_cast<Int64>(reference_time), 50);

    for (UInt32 year : {base_year - 100, base_year, base_year + 100})
    {
        std::optional<time_t> res = makeTimestamp(year, month, day, time);
        if (res && *res > window_begin && *res <= window_end)
            return res;
    }

    return std::nullopt;
}

std::optional<time_t> tryParseRFC850Date(std::string_view date, time_t reference_time)
{
    const auto day_name_it = std::ranges::find_if(
        long_day_names,
        [&](std::string_view name) { return date.size() >= name.size() && equalsCaseInsensitive(date.substr(0, name.size()), name); });
    if (day_name_it == long_day_names.end())
        return std::nullopt;

    std::string_view rest = date.substr(day_name_it->size());
    if (rest.size() != rfc850_date_rest_example.size())
        return std::nullopt;

    if (!rest.starts_with(", ") || rest[4] != '-' || rest[8] != '-' || rest[11] != ' ' || rest[20] != ' '
        || !equalsCaseInsensitive(rest.substr(21), "GMT"))
        return std::nullopt;

    std::optional<UInt8> month = parseMonth(rest.substr(5, 3));
    std::optional<UInt32> day = parseNumber(rest, 2, 2);
    std::optional<UInt32> two_digit_year = parseNumber(rest, 9, 2);
    std::optional<TimeOfDay> time = parseTimeOfDay(rest, 12);

    if (!month || !day || !two_digit_year || !time)
        return std::nullopt;

    return makeTimestampFromTwoDigitYear(*two_digit_year, *month, *day, *time, reference_time);
}

std::optional<time_t> tryParseAsctimeDate(std::string_view date)
{
    if (date.size() != asctime_date_example.size())
        return std::nullopt;

    if (date[3] != ' ' || date[7] != ' ' || date[10] != ' ' || date[19] != ' ')
        return std::nullopt;

    if (!isDayName(date.substr(0, 3)))
        return std::nullopt;

    std::optional<UInt8> month = parseMonth(date.substr(4, 3));
    std::optional<UInt32> day = date[8] == ' ' ? parseNumber(date, 9, 1) : parseNumber(date, 8, 2);
    std::optional<TimeOfDay> time = parseTimeOfDay(date, 11);
    std::optional<UInt32> year = parseNumber(date, 20, 4);

    if (!month || !day || !time || !year)
        return std::nullopt;

    return makeTimestamp(*year, *month, *day, *time);
}

}

std::optional<time_t> tryParseHTTPDate(std::string_view date, time_t reference_time)
{
    if (std::optional<time_t> res = tryParseIMFFixdate(date))
        return res;
    if (std::optional<time_t> res = tryParseRFC850Date(date, reference_time))
        return res;
    return tryParseAsctimeDate(date);
}

std::optional<time_t> tryParseHTTPDate(std::string_view date)
{
    return tryParseHTTPDate(date, ::time(nullptr));
}

}
