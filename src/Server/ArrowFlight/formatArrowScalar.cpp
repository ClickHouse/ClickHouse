#include <Server/ArrowFlight/formatArrowScalar.h>

#include <base/extended_types.h>
#include <base/wide_integer_to_string.h>
#include <Common/Exception.h>

#include <double-conversion/double-conversion.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ArrowFlight
{

namespace
{

/// The Apache Arrow library formats the days of the years from -32767 to 32767, and reports the values outside
/// of them as out of range.
constexpr Int64 MIN_DAY = -12687428;
constexpr Int64 END_DAY = 11248738;

constexpr Int64 SECONDS_PER_DAY = 86400;

/// The parameters of the converter the Apache Arrow library formats floating-point numbers with.
const double_conversion::DoubleToStringConverter & floatConverter()
{
    static const double_conversion::DoubleToStringConverter converter(
        double_conversion::DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN,
        "inf",
        "nan",
        'e',
        /* decimal_in_shortest_low */ -6,
        /* decimal_in_shortest_high */ 10,
        /* max_leading_padding_zeroes_in_precision_mode */ 6,
        /* max_trailing_padding_zeroes_in_precision_mode */ 0);
    return converter;
}

/// Enough for the shortest text of any `double`.
constexpr size_t FLOAT_BUFFER_SIZE = 50;

String outOfRange(Int64 value)
{
    return fmt::format("<value out of range: {}>", value);
}

Int64 unitsPerSecond(ArrowTimeUnit unit)
{
    switch (unit)
    {
        case ArrowTimeUnit::Second: return 1;
        case ArrowTimeUnit::Millisecond: return 1'000;
        case ArrowTimeUnit::Microsecond: return 1'000'000;
        case ArrowTimeUnit::Nanosecond: return 1'000'000'000;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown Arrow time unit {}", static_cast<UInt32>(unit));
}

/// `YYYY-MM-DD` of a day since 1970-01-01 in the proleptic Gregorian calendar
/// (http://howardhinnant.github.io/date_algorithms.html#civil_from_days).
String formatDay(Int64 day)
{
    const Int64 shifted_day = day + 719468;
    const Int64 era = (shifted_day >= 0 ? shifted_day : shifted_day - 146096) / 146097;
    const Int64 day_of_era = shifted_day - era * 146097;
    const Int64 year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    const Int64 day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    const Int64 shifted_month = (5 * day_of_year + 2) / 153;
    const Int64 day_of_month = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    const Int64 month = shifted_month < 10 ? shifted_month + 3 : shifted_month - 9;
    const Int64 year = year_of_era + era * 400 + (month <= 2);
    /// The year has at least four digits, and a sign only if it is negative.
    return fmt::format("{}{:04}-{:02}-{:02}", year < 0 ? "-" : "", year < 0 ? -year : year, month, day_of_month);
}

/// `HH:MM:SS[.fraction]` of a number of units since midnight: not negative, and less than a day.
String formatTimeOfDay(Int64 since_midnight, ArrowTimeUnit unit)
{
    const Int64 units_per_second = unitsPerSecond(unit);
    const Int64 seconds = since_midnight / units_per_second;
    String result = fmt::format("{:02}:{:02}:{:02}", seconds / 3600, seconds / 60 % 60, seconds % 60);
    /// Three digits for milliseconds, six for microseconds, nine for nanoseconds.
    if (const size_t subsecond_digits = static_cast<size_t>(unit) * 3)
        result += fmt::format(".{:0{}}", since_midnight % units_per_second, subsecond_digits);
    return result;
}

}

String formatArrowFloat32(Float32 value)
{
    char buffer[FLOAT_BUFFER_SIZE];
    double_conversion::StringBuilder builder(buffer, sizeof(buffer));
    if (!floatConverter().ToShortestSingle(value, &builder))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot format a floating-point number");
    return String(buffer, static_cast<size_t>(builder.position()));
}

String formatArrowFloat64(Float64 value)
{
    char buffer[FLOAT_BUFFER_SIZE];
    double_conversion::StringBuilder builder(buffer, sizeof(buffer));
    if (!floatConverter().ToShortest(value, &builder))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot format a floating-point number");
    return String(buffer, static_cast<size_t>(builder.position()));
}

String formatArrowDate32(Int32 days)
{
    if (days < MIN_DAY || days >= END_DAY)
        return outOfRange(days);
    return formatDay(days);
}

String formatArrowDate64(Int64 milliseconds)
{
    constexpr Int64 milliseconds_per_day = SECONDS_PER_DAY * 1000;
    if (milliseconds < MIN_DAY * milliseconds_per_day || milliseconds >= END_DAY * milliseconds_per_day)
        return outOfRange(milliseconds);
    /// Rounded towards zero, as the Apache Arrow library does: the value is supposed to be a whole number of days.
    return formatDay(milliseconds / milliseconds_per_day);
}

String formatArrowTimestamp(Int64 value, ArrowTimeUnit unit, bool has_time_zone)
{
    const Int64 units_per_day = unitsPerSecond(unit) * SECONDS_PER_DAY;
    /// Every number of nanoseconds is in the range (its bounds do not even fit into 64 bits of nanoseconds).
    if (unit != ArrowTimeUnit::Nanosecond && (value < MIN_DAY * units_per_day || value >= END_DAY * units_per_day))
        return outOfRange(value);

    /// The day is rounded down, so that the time of the day of a timestamp before 1970 is not negative.
    Int64 day = value / units_per_day;
    Int64 since_midnight = value % units_per_day;
    if (since_midnight < 0)
    {
        --day;
        since_midnight += units_per_day;
    }

    String result = formatDay(day);
    result += ' ';
    result += formatTimeOfDay(since_midnight, unit);
    if (has_time_zone)
        result += 'Z';
    return result;
}

String formatArrowTime(Int64 value, ArrowTimeUnit unit)
{
    if (value < 0 || value >= unitsPerSecond(unit) * SECONDS_PER_DAY)
        return outOfRange(value);
    return formatTimeOfDay(value, unit);
}

String formatArrowDecimal(std::string_view coefficient, Int32 scale)
{
    const size_t size = coefficient.size();
    if (size != 4 && size != 8 && size != 16 && size != 32)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected size {} of the value of an Arrow decimal", size);

    /// The maximum precision of the type: 9, 18, 38 or 76 digits.
    const Int32 max_scale = size == 4 ? 9 : (size == 8 ? 18 : (size == 16 ? 38 : 76));
    if (scale < -max_scale || scale > max_scale)
        return fmt::format("<scale out of range, cannot format Decimal{} value>", size * 8);

    /// The value extended with its sign to 256 bits.
    Int256 value = (static_cast<UInt8>(coefficient.back()) & 0x80) ? -1 : 0;
    for (size_t i = size; i > 0; --i)
        value = (value << 8) | Int256(static_cast<UInt8>(coefficient[i - 1]));

    String text = wide::to_string(value);
    if (scale == 0)
        return text;

    const Int32 sign_size = text.front() == '-';
    const Int32 num_digits = static_cast<Int32>(text.size()) - sign_size;
    const Int32 adjusted_exponent = num_digits - 1 - scale;

    /// The scientific notation: `1.23E+4`, `-1.23E-7`, `0E+1`. The limit of -6 is the one of `BigDecimal` of Java.
    if (scale < 0 || adjusted_exponent < -6)
    {
        if (num_digits > 1)
            text.insert(static_cast<size_t>(sign_size) + 1, 1, '.');
        text += 'E';
        if (adjusted_exponent >= 0)
            text += '+';
        text += fmt::format("{}", adjusted_exponent);
        return text;
    }

    /// `12.3`
    if (num_digits > scale)
    {
        text.insert(text.size() - static_cast<size_t>(scale), 1, '.');
        return text;
    }

    /// `0.0123`: the scale is not less than the number of digits here.
    const size_t leading_zeros = static_cast<size_t>(scale) - static_cast<size_t>(num_digits) + 2;
    text.insert(static_cast<size_t>(sign_size), leading_zeros, '0');
    text[static_cast<size_t>(sign_size) + 1] = '.';
    return text;
}

String formatArrowMonthInterval(Int32 months)
{
    return fmt::format("{}M", months);
}

String formatArrowDayTimeInterval(Int32 days, Int32 milliseconds)
{
    return fmt::format("{}d{}ms", days, milliseconds);
}

String formatArrowMonthDayNanoInterval(Int32 months, Int32 days, Int64 nanoseconds)
{
    return fmt::format("{}M{}d{}ns", months, days, nanoseconds);
}

}

}
