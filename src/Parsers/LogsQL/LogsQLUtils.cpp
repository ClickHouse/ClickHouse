#include <Parsers/LogsQL/LogsQLUtils.h>

#include <base/arithmeticOverflow.h>
#include <Common/StringUtils.h>

#include <fmt/format.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <limits>

namespace DB
{

namespace LogsQLUtils
{

namespace
{

constexpr Int64 nsecs_per_microsecond = 1000;
constexpr Int64 nsecs_per_millisecond = 1000 * nsecs_per_microsecond;
constexpr Int64 nsecs_per_second = 1000 * nsecs_per_millisecond;
constexpr Int64 nsecs_per_minute = 60 * nsecs_per_second;
constexpr Int64 nsecs_per_hour = 60 * nsecs_per_minute;
constexpr Int64 nsecs_per_day = 24 * nsecs_per_hour;
constexpr Int64 nsecs_per_week = 7 * nsecs_per_day;
constexpr Int64 nsecs_per_year = 365 * nsecs_per_day;

bool isDigit(char c)
{
    return c >= '0' && c <= '9';
}

/// Removes '_' digit separators: "1_234_567" -> "1234567".
String removeUnderscores(const String & text)
{
    String result;
    result.reserve(text.size());
    for (char c : text)
        if (c != '_')
            result += c;
    return result;
}

/// Parses a plain decimal float (possibly with '_' separators and an exponent) from the whole string.
std::optional<Float64> tryParsePlainFloat(const String & text_with_underscores)
{
    if (text_with_underscores.empty())
        return {};

    String text = removeUnderscores(text_with_underscores);
    Float64 value = 0;
    const char * first = text.data();
    const char * last = text.data() + text.size();
    auto [end, ec] = std::from_chars(first, last, value);
    if (ec != std::errc() || end != last)
        return {};
    return value;
}

/// Parses a float prefix of the string. Returns the value and the rest of the string.
std::optional<Float64> tryParseFloatPrefix(std::string_view & text)
{
    size_t size = 0;
    if (size < text.size() && (text[size] == '-' || text[size] == '+'))
        ++size;
    size_t digits_begin = size;
    while (size < text.size() && (isDigit(text[size]) || text[size] == '_'))
        ++size;
    if (size < text.size() && text[size] == '.')
    {
        ++size;
        while (size < text.size() && (isDigit(text[size]) || text[size] == '_'))
            ++size;
    }
    if (size == digits_begin)
        return {};

    auto value = tryParsePlainFloat(String(text.substr(0, size)));
    if (!value)
        return {};
    text.remove_prefix(size);
    return value;
}

std::optional<Float64> tryParseIntWithBasePrefix(const String & text_with_underscores)
{
    String text = removeUnderscores(text_with_underscores);
    std::string_view rest = text;

    bool negative = false;
    if (!rest.empty() && (rest[0] == '-' || rest[0] == '+'))
    {
        negative = rest[0] == '-';
        rest.remove_prefix(1);
    }

    int base = 0;
    if (rest.size() > 2 && rest[0] == '0' && (rest[1] == 'x' || rest[1] == 'X'))
        base = 16;
    else if (rest.size() > 2 && rest[0] == '0' && (rest[1] == 'b' || rest[1] == 'B'))
        base = 2;
    else if (rest.size() > 2 && rest[0] == '0' && (rest[1] == 'o' || rest[1] == 'O'))
        base = 8;
    else
        return {};

    rest.remove_prefix(2);
    UInt64 value = 0;
    auto [end, ec] = std::from_chars(rest.data(), rest.data() + rest.size(), value, base);
    if (ec != std::errc() || end != rest.data() + rest.size())
        return {};
    Float64 result = static_cast<Float64>(value);
    return negative ? -result : result;
}

struct SuffixMultiplier
{
    std::string_view suffix;
    Float64 multiplier;
};

/// Byte size suffixes, longest match first.
constexpr SuffixMultiplier byte_suffixes[] = {
    {"KiB", 1ULL << 10}, {"MiB", 1ULL << 20}, {"GiB", 1ULL << 30}, {"TiB", 1ULL << 40},
    {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
    {"Ki", 1ULL << 10}, {"Mi", 1ULL << 20}, {"Gi", 1ULL << 30}, {"Ti", 1ULL << 40},
    {"K", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
    {"B", 1},
};

std::optional<Float64> tryParseBytes(const String & text)
{
    if (text.empty())
        return {};

    std::string_view rest = text;
    bool negative = false;
    if (rest[0] == '-')
    {
        negative = true;
        rest.remove_prefix(1);
    }

    Float64 total = 0;
    bool has_suffix = false;
    while (!rest.empty())
    {
        auto value = tryParseFloatPrefix(rest);
        if (!value)
            return {};

        if (rest.empty())
        {
            /// A trailing number without a suffix must be an integer.
            Float64 int_part = 0;
            if (std::modf(*value, &int_part) != 0.0)
                return {};
            total += *value;
            break;
        }

        bool matched = false;
        for (const auto & entry : byte_suffixes)
        {
            if (rest.starts_with(entry.suffix))
            {
                total += *value * entry.multiplier;
                rest.remove_prefix(entry.suffix.size());
                matched = true;
                has_suffix = true;
                break;
            }
        }
        if (!matched)
            return {};
    }

    if (!has_suffix)
        return {};
    return negative ? -total : total;
}

constexpr SuffixMultiplier duration_suffixes[] = {
    {"\xC2\xB5s", nsecs_per_microsecond},  /// µs
    {"ms", nsecs_per_millisecond},
    {"ns", 1},
    {"y", nsecs_per_year},
    {"w", nsecs_per_week},
    {"d", nsecs_per_day},
    {"h", nsecs_per_hour},
    {"m", nsecs_per_minute},
    {"s", nsecs_per_second},
};

}

std::optional<Int64> tryParseDuration(const String & text)
{
    if (text.empty())
        return {};

    std::string_view rest = text;
    bool negative = false;
    if (rest[0] == '-')
    {
        negative = true;
        rest.remove_prefix(1);
    }
    if (rest.empty())
        return {};

    Float64 total = 0;
    while (!rest.empty())
    {
        auto value = tryParseFloatPrefix(rest);
        if (!value || rest.empty())
            return {};

        bool matched = false;
        for (const auto & entry : duration_suffixes)
        {
            if (rest.starts_with(entry.suffix))
            {
                total += *value * entry.multiplier;
                rest.remove_prefix(entry.suffix.size());
                matched = true;
                break;
            }
        }
        if (!matched)
            return {};
    }

    if (std::abs(total) > 9e18)
        return {};
    Int64 result = static_cast<Int64>(total);
    return negative ? -result : result;
}

std::optional<Float64> tryParseNumber(const String & text)
{
    if (text.empty())
        return {};

    /// inf and nan.
    {
        std::string_view rest = text;
        bool negative = false;
        if (rest[0] == '-' || rest[0] == '+')
        {
            negative = rest[0] == '-';
            rest.remove_prefix(1);
        }
        String lower;
        for (char c : rest)
            lower += toLowerASCII(c);
        if (lower == "inf" || lower == "infinity")
            return negative ? -INFINITY : INFINITY;
        if (lower == "nan")
            return NAN;
    }

    if (auto value = tryParsePlainFloat(text))
        return value;

    /// Exponential form is not handled by tryParsePlainFloat prefix logic above only when underscores
    /// are present, but from_chars handles "1e3" directly, so nothing else to do for it here.

    if (auto value = tryParseIntWithBasePrefix(text))
        return value;

    if (auto duration = tryParseDuration(text))
        return static_cast<Float64>(*duration);

    if (auto bytes = tryParseBytes(text))
        return bytes;

    return {};
}

namespace
{

/// Converts a non-negative exact magnitude and a sign into an integer field.
/// Returns nullopt when the value does not fit the 64-bit range.
std::optional<Field> integerFieldFromMagnitude(Int128 magnitude, bool negative)
{
    if (!negative)
    {
        if (magnitude > Int128(std::numeric_limits<UInt64>::max()))
            return {};
        return Field(static_cast<UInt64>(magnitude));
    }
    if (-magnitude < Int128(std::numeric_limits<Int64>::min()))
        return {};
    return Field(static_cast<Int64>(-magnitude));
}

/// Computes suffixed terms ("1h30m", "10KiB", "1Ki512") with integer arithmetic.
/// Returns nullopt when any term is fractional, so the caller falls back to the
/// Float64 value of tryParseNumber. The sign must already be stripped.
std::optional<Int128> tryParseExactSuffixedTerms(std::string_view rest, bool durations)
{
    const SuffixMultiplier * suffixes = durations ? duration_suffixes : byte_suffixes;
    size_t suffix_count = durations ? std::size(duration_suffixes) : std::size(byte_suffixes);

    Int128 total = 0;
    bool has_suffix = false;
    while (!rest.empty())
    {
        size_t digits = 0;
        while (digits < rest.size() && isDigit(rest[digits]))
            ++digits;
        if (digits == 0)
            return {};
        std::string_view digits_view = rest.substr(0, digits);
        UInt64 value = 0;
        auto [end, ec] = std::from_chars(digits_view.data(), digits_view.data() + digits_view.size(), value);
        if (ec != std::errc() || end != digits_view.data() + digits_view.size())
            return {};
        rest.remove_prefix(digits);

        if (rest.empty())
        {
            /// A trailing number without a suffix is allowed only in byte sizes.
            if (durations || !has_suffix)
                return {};
            total += Int128(value);
            break;
        }

        bool matched = false;
        for (size_t i = 0; i < suffix_count; ++i)
        {
            if (rest.starts_with(suffixes[i].suffix))
            {
                /// All the multipliers (powers of 2 and of 10 up to 1e12, and the nanosecond
                /// unit factors) are integers exactly representable in Float64.
                total += Int128(value) * static_cast<Int128>(suffixes[i].multiplier);
                rest.remove_prefix(suffixes[i].suffix.size());
                matched = true;
                has_suffix = true;
                break;
            }
        }
        if (!matched)
            return {};
        if (total > Int128(std::numeric_limits<UInt64>::max()))
            return {};
    }
    if (!has_suffix)
        return {};
    return total;
}

}

std::optional<Field> tryParseNumberField(const String & text)
{
    auto approximate = tryParseNumber(text);
    if (!approximate)
        return {};

    String stripped = removeUnderscores(text);
    std::string_view rest = stripped;
    bool negative = false;
    if (!rest.empty() && (rest[0] == '-' || rest[0] == '+'))
    {
        negative = rest[0] == '-';
        rest.remove_prefix(1);
    }

    if (!rest.empty() && isDigit(rest[0]))
    {
        /// A plain decimal integer.
        if (std::all_of(rest.begin(), rest.end(), isDigit))
        {
            UInt64 value = 0;
            auto [end, ec] = std::from_chars(rest.data(), rest.data() + rest.size(), value);
            if (ec == std::errc() && end == rest.data() + rest.size())
                if (auto field = integerFieldFromMagnitude(Int128(value), negative))
                    return field;
        }
        /// An integer with a base prefix (0x/0o/0b).
        else if (rest.size() > 2 && rest[0] == '0'
            && (rest[1] == 'x' || rest[1] == 'X' || rest[1] == 'o' || rest[1] == 'O' || rest[1] == 'b' || rest[1] == 'B'))
        {
            int base = (rest[1] == 'x' || rest[1] == 'X') ? 16 : ((rest[1] == 'o' || rest[1] == 'O') ? 8 : 2);
            std::string_view sub = rest.substr(2);
            UInt64 value = 0;
            auto [end, ec] = std::from_chars(sub.data(), sub.data() + sub.size(), value, base);
            if (ec == std::errc() && end == sub.data() + sub.size())
                if (auto field = integerFieldFromMagnitude(Int128(value), negative))
                    return field;
        }
        /// Integral duration or byte-size terms. Durations are tried first,
        /// in the same order as tryParseNumber, and their suffix sets do not
        /// overlap on any complete string, so the value matches tryParseNumber.
        else
        {
            for (bool durations : {true, false})
                if (auto magnitude = tryParseExactSuffixedTerms(rest, durations))
                    if (auto field = integerFieldFromMagnitude(*magnitude, negative))
                        return field;
        }
    }

    return Field(*approximate);
}

bool isNumberPrefix(const String & text)
{
    std::string_view rest = text;
    if (rest.empty())
        return false;
    if (rest[0] == '-' || rest[0] == '+')
    {
        rest.remove_prefix(1);
        if (rest.empty())
            return false;
    }
    if (rest.starts_with("inf") || rest.starts_with("Inf"))
        return true;
    return isDigit(rest[0]) || (rest.size() >= 2 && rest[0] == '.' && isDigit(rest[1]));
}

String escapeRegexp(const String & text)
{
    String result;
    result.reserve(text.size());
    for (char c : text)
    {
        bool is_word = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
            || static_cast<unsigned char>(c) >= 0x80;
        if (!is_word)
            result += '\\';
        result += c;
    }
    return result;
}

std::optional<UInt32> tryParseIPv4(const String & text)
{
    UInt32 result = 0;
    size_t pos = 0;
    for (size_t octet = 0; octet < 4; ++octet)
    {
        if (octet > 0)
        {
            if (pos >= text.size() || text[pos] != '.')
                return {};
            ++pos;
        }
        if (pos >= text.size() || !isDigit(text[pos]))
            return {};
        UInt32 value = 0;
        size_t digits = 0;
        while (pos < text.size() && isDigit(text[pos]) && digits < 4)
        {
            value = value * 10 + (text[pos] - '0');
            ++pos;
            ++digits;
        }
        if (value > 255)
            return {};
        result = (result << 8) | value;
    }
    if (pos != text.size())
        return {};
    return result;
}

namespace
{

unsigned daysInMonth(Int64 year, unsigned month)
{
    static constexpr unsigned days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    if (month == 2 && leap)
        return 29;
    return days[month - 1];
}

/// Days from the civil epoch 1970-01-01 to the given date. Howard Hinnant's algorithm.
Int64 daysFromCivil(Int64 year, unsigned month, unsigned day)
{
    year -= month <= 2;
    const Int64 era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(year - era * 400);
    const unsigned doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<Int64>(doe) - 719468;
}

void civilFromDays(Int64 days, Int64 & year, unsigned & month, unsigned & day)
{
    days += 719468;
    const Int64 era = (days >= 0 ? days : days - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(days - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const Int64 y = static_cast<Int64>(yoe) + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    day = doy - (153 * mp + 2) / 5 + 1;
    month = mp + (mp < 10 ? 3 : -9);
    year = y + (month <= 2);
}

struct CivilTime
{
    Int64 year = 1970;
    unsigned month = 1;
    unsigned day = 1;
    unsigned hour = 0;
    unsigned minute = 0;
    unsigned second = 0;
    UInt32 nanosecond = 0;

    Int64 toNanoseconds() const
    {
        Int64 days = daysFromCivil(year, month, day);
        Int64 seconds = days * 86400 + hour * 3600 + minute * 60 + second;
        return seconds * nsecs_per_second + nanosecond;
    }
};

enum class TimePrecision
{
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Subsecond,
};

/// Computes the end (exclusive) of the period starting at the given time with the given precision.
CivilTime periodEnd(CivilTime t, TimePrecision precision)
{
    switch (precision)
    {
        case TimePrecision::Year:
            ++t.year;
            return t;
        case TimePrecision::Month:
            if (t.month == 12)
            {
                t.month = 1;
                ++t.year;
            }
            else
                ++t.month;
            return t;
        default:
            break;
    }

    Int64 step_ns = 0;
    switch (precision)
    {
        case TimePrecision::Day: step_ns = nsecs_per_day; break;
        case TimePrecision::Hour: step_ns = nsecs_per_hour; break;
        case TimePrecision::Minute: step_ns = nsecs_per_minute; break;
        case TimePrecision::Second: step_ns = nsecs_per_second; break;
        case TimePrecision::Subsecond: step_ns = 1; break;
        default: break;
    }

    Int64 ns = t.toNanoseconds() + step_ns;
    CivilTime result;
    Int64 days = ns / nsecs_per_day;
    Int64 in_day = ns % nsecs_per_day;
    if (in_day < 0)
    {
        in_day += nsecs_per_day;
        --days;
    }
    civilFromDays(days, result.year, result.month, result.day);
    result.hour = static_cast<unsigned>(in_day / nsecs_per_hour);
    result.minute = static_cast<unsigned>(in_day % nsecs_per_hour / nsecs_per_minute);
    result.second = static_cast<unsigned>(in_day % nsecs_per_minute / nsecs_per_second);
    result.nanosecond = static_cast<UInt32>(in_day % nsecs_per_second);
    return result;
}

String formatCivil(const CivilTime & t)
{
    String result = fmt::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", t.year, t.month, t.day, t.hour, t.minute, t.second);
    if (t.nanosecond != 0)
        result += fmt::format(".{:09}", t.nanosecond);
    return result;
}

bool parseFixedNumber(const String & text, size_t & pos, size_t digits, unsigned & result)
{
    if (text.size() - pos < digits)
        return false;
    unsigned value = 0;
    for (size_t i = 0; i < digits; ++i)
    {
        if (!isDigit(text[pos + i]))
            return false;
        value = value * 10 + (text[pos + i] - '0');
    }
    pos += digits;
    result = value;
    return true;
}

}

std::optional<TimeValue> tryParseTimestamp(const String & text)
{
    if (text.empty())
        return {};

    /// Unix timestamps: interpret by the number of digits (seconds, milliseconds, microseconds, nanoseconds).
    {
        bool all_digits = true;
        for (char c : text)
            if (!isDigit(c) && c != '.')
                all_digits = false;
        if (all_digits && !text.contains('.') && text.size() >= 5)
        {
            UInt64 value = 0;
            auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
            if (ec == std::errc() && end == text.data() + text.size())
            {
                /// The unit is inferred from the number of digits. A value beyond the Int64
                /// nanosecond range (the year 2262) is rejected instead of silently
                /// overflowing into a wrong epoch.
                constexpr auto max_ns = std::numeric_limits<Int64>::max();
                Int64 ns = 0;
                Int64 unit_ns = 0;
                if (value < 100'000'000'000ULL)
                    unit_ns = nsecs_per_second;
                else if (value < 100'000'000'000'000ULL)
                    unit_ns = nsecs_per_millisecond;
                else if (value < 100'000'000'000'000'000ULL)
                    unit_ns = nsecs_per_microsecond;
                else
                    unit_ns = 1;
                if (value > static_cast<UInt64>(max_ns / unit_ns))
                    return {};
                ns = static_cast<Int64>(value) * unit_ns;

                /// The timestamp denotes the whole period of its implied precision:
                /// e.g. a millisecond timestamp matches the whole millisecond.
                TimeValue result;
                result.has_timezone = true;
                result.start_ns = ns;
                result.end_ns = ns <= max_ns - unit_ns ? ns + unit_ns : max_ns;
                return result;
            }
        }
    }

    /// A trailing `+hh:mm`/`-hh:mm` timezone offset may directly follow any timestamp prefix,
    /// e.g. `2024-01-02:30` is the month `2024-01` with the offset `-02:30`.
    /// It is detached here beforehand, since otherwise its first number would be consumed
    /// as the next timestamp component.
    String base = text;
    std::optional<Int64> detached_offset_ns;
    if (text.size() > 6)
    {
        char sign = text[text.size() - 6];
        if ((sign == '+' || sign == '-') && text[text.size() - 3] == ':'
            && isDigit(text[text.size() - 5]) && isDigit(text[text.size() - 4])
            && isDigit(text[text.size() - 2]) && isDigit(text[text.size() - 1]))
        {
            Int64 tz_hour = (text[text.size() - 5] - '0') * 10 + (text[text.size() - 4] - '0');
            Int64 tz_minute = (text[text.size() - 2] - '0') * 10 + (text[text.size() - 1] - '0');
            if (tz_hour <= 23 && tz_minute <= 59)
            {
                detached_offset_ns = (tz_hour * 3600 + tz_minute * 60) * nsecs_per_second;
                if (sign == '-')
                    detached_offset_ns = -*detached_offset_ns;
                base = text.substr(0, text.size() - 6);
            }
        }
    }

    CivilTime t;
    TimePrecision precision = TimePrecision::Year;
    size_t pos = 0;

    unsigned year = 0;
    if (!parseFixedNumber(base, pos, 4, year))
        return {};
    /// Nanoseconds since the epoch are carried in Int64 (the DateTime64(9) range,
    /// years 1678..2261); years outside it would overflow the epoch arithmetic.
    if (year < 1678 || year > 2261)
        return {};
    t.year = year;

    auto parse_component = [&](char separator, size_t digits, unsigned & value, TimePrecision component_precision)
    {
        if (pos >= base.size() || base[pos] != separator)
            return false;
        size_t saved_pos = pos;
        ++pos;
        if (!parseFixedNumber(base, pos, digits, value))
        {
            pos = saved_pos;
            return false;
        }
        precision = component_precision;
        return true;
    };

    if (parse_component('-', 2, t.month, TimePrecision::Month))
    {
        if (t.month < 1 || t.month > 12)
            return {};
        if (parse_component('-', 2, t.day, TimePrecision::Day))
        {
            if (t.day < 1 || t.day > daysInMonth(t.year, t.month))
                return {};
            if (parse_component('T', 2, t.hour, TimePrecision::Hour) || parse_component('t', 2, t.hour, TimePrecision::Hour))
            {
                if (t.hour > 23)
                    return {};
                if (parse_component(':', 2, t.minute, TimePrecision::Minute))
                {
                    if (t.minute > 59)
                        return {};
                    if (parse_component(':', 2, t.second, TimePrecision::Second))
                    {
                        if (t.second > 59)
                            return {};
                        if (pos < base.size() && base[pos] == '.')
                        {
                            ++pos;
                            size_t digits = 0;
                            UInt64 fraction = 0;
                            while (pos < base.size() && isDigit(base[pos]) && digits < 9)
                            {
                                fraction = fraction * 10 + (base[pos] - '0');
                                ++pos;
                                ++digits;
                            }
                            if (digits == 0)
                                return {};
                            for (size_t i = digits; i < 9; ++i)
                                fraction *= 10;
                            t.nanosecond = static_cast<UInt32>(fraction);
                            precision = TimePrecision::Subsecond;
                        }
                    }
                }
            }
        }
    }

    /// Timezone suffix: Z, +hh:mm, -hh:mm, +hh, -hh, or nothing.
    bool has_timezone = false;
    Int64 timezone_offset_ns = 0;
    if (detached_offset_ns)
    {
        has_timezone = true;
        timezone_offset_ns = *detached_offset_ns;
    }
    else if (pos < base.size())
    {
        char c = base[pos];
        if (c == 'Z' || c == 'z')
        {
            has_timezone = true;
            ++pos;
        }
        else if (c == '+' || c == '-')
        {
            ++pos;
            unsigned tz_hour = 0;
            unsigned tz_minute = 0;
            if (!parseFixedNumber(base, pos, 2, tz_hour))
                return {};
            if (pos >= base.size() || base[pos] != ':')
                return {};
            ++pos;
            if (!parseFixedNumber(base, pos, 2, tz_minute))
                return {};
            if (tz_hour > 23 || tz_minute > 59)
                return {};
            timezone_offset_ns = (static_cast<Int64>(tz_hour) * 3600 + tz_minute * 60) * nsecs_per_second;
            if (c == '-')
                timezone_offset_ns = -timezone_offset_ns;
            has_timezone = true;
        }
    }

    if (pos != base.size())
        return {};

    CivilTime end = periodEnd(t, precision);

    TimeValue result;
    result.has_timezone = has_timezone;
    if (has_timezone)
    {
        result.start_ns = t.toNanoseconds() - timezone_offset_ns;
        result.end_ns = end.toNanoseconds() - timezone_offset_ns;
    }
    else
    {
        result.start_civil = formatCivil(t);
        result.end_civil = formatCivil(end);
    }
    return result;
}

String formatTimestampUTC(Int64 ns)
{
    CivilTime t;
    Int64 days = ns / nsecs_per_day;
    Int64 in_day = ns % nsecs_per_day;
    if (in_day < 0)
    {
        in_day += nsecs_per_day;
        --days;
    }
    civilFromDays(days, t.year, t.month, t.day);
    t.hour = static_cast<unsigned>(in_day / nsecs_per_hour);
    t.minute = static_cast<unsigned>(in_day % nsecs_per_hour / nsecs_per_minute);
    t.second = static_cast<unsigned>(in_day % nsecs_per_minute / nsecs_per_second);
    t.nanosecond = static_cast<UInt32>(in_day % nsecs_per_second);
    return formatCivil(t);
}

}

}
