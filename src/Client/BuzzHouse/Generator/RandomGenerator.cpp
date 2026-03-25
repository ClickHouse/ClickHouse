#include <Client/BuzzHouse/Generator/RandomGenerator.h>

#include <fmt/format.h>

namespace BuzzHouse
{

uint64_t RandomGenerator::getSeed() const
{
    return seed;
}

uint32_t RandomGenerator::nextSmallNumber()
{
    return dist1(generator);
}

uint32_t RandomGenerator::nextMediumNumber()
{
    return dist2(generator);
}

uint32_t RandomGenerator::nextLargeNumber()
{
    return dist3(generator);
}

uint8_t RandomGenerator::nextRandomUInt8()
{
    return uints8(generator);
}

int8_t RandomGenerator::nextRandomInt8()
{
    return ints8(generator);
}

uint16_t RandomGenerator::nextRandomUInt16()
{
    return uints16(generator);
}

int16_t RandomGenerator::nextRandomInt16()
{
    return ints16(generator);
}

uint32_t RandomGenerator::nextRandomUInt32()
{
    return uints32(generator);
}

int32_t RandomGenerator::nextRandomInt32()
{
    return ints32(generator);
}

uint64_t RandomGenerator::nextRandomUInt64()
{
    return uints64(generator);
}

int64_t RandomGenerator::nextRandomInt64()
{
    return ints64(generator);
}

uint64_t RandomGenerator::nextInFullRange()
{
    return full_range(generator);
}

uint32_t RandomGenerator::nextStrlen()
{
    return strlens(generator);
}

char RandomGenerator::nextDigit()
{
    return static_cast<char>(digits(generator));
}

bool RandomGenerator::nextBool()
{
    return dist4(generator) == 2;
}

String RandomGenerator::nextDate(const String & separator, const bool allow_func)
{
    if (allow_func && this->nextMediumNumber() < 16)
    {
        return "today()";
    }
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 1))
        {
            case 0:
                return separator + "1970-01-01" + separator; /// Epoch / min Date
            case 1:
                return separator + "2149-06-06" + separator; /// Max Date
            default:
                UNREACHABLE();
        }
    }
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    return fmt::format(
        "{}{}-{}{}-{}{}{}", separator, 1970 + date_years(generator), month < 10 ? "0" : "", month, day < 10 ? "0" : "", day, separator);
}

String RandomGenerator::nextDate32(const String & separator, const bool allow_func)
{
    if (allow_func && this->nextMediumNumber() < 16)
    {
        return "today()";
    }
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 2))
        {
            case 0:
                return separator + "1900-01-01" + separator; /// Min Date32
            case 1:
                return separator + "2299-12-31" + separator; /// Max Date32
            case 2:
                return separator + "1970-01-01" + separator; /// Epoch
            default:
                UNREACHABLE();
        }
    }
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    return fmt::format(
        "{}{}-{}{}-{}{}{}",
        separator,
        1900 + datetime64_years(generator),
        month < 10 ? "0" : "",
        month,
        day < 10 ? "0" : "",
        day,
        separator);
}

String RandomGenerator::nextTime(const String & separator, const bool allow_func)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 3))
        {
            case 0:
                return separator + "00:00:00" + separator; /// Midnight
            case 1:
                return separator + "23:59:59" + separator; /// End of day
            case 2:
                return separator + "-999:59:59" + separator; /// Min Time
            case 3:
                return separator + "999:59:59" + separator; /// Max Time
            default:
                UNREACHABLE();
        }
    }
    const int32_t hour = time_hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);
    return fmt::format("{}{}:{}{}:{}{}{}", separator, hour, minute < 10 ? "0" : "", minute, second < 10 ? "0" : "", second, separator);
}

String RandomGenerator::nextTime64(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    if (this->nextMediumNumber() < 16)
    {
        const String sub = has_subseconds ? ".999999999" : "";
        const String sub0 = has_subseconds ? ".000000000" : "";
        switch (this->randomInt<uint32_t>(0, 3))
        {
            case 0:
                return separator + "00:00:00" + sub0 + separator; /// Midnight
            case 1:
                return separator + "23:59:59" + sub + separator; /// End of day (with max subseconds)
            case 2:
                return separator + "-999:59:59" + sub0 + separator; /// Min Time64
            case 3:
                return separator + "999:59:59" + sub + separator; /// Max Time64
            default:
                UNREACHABLE();
        }
    }
    const int32_t hour = time_hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);
    return fmt::format(
        "{}{}:{}{}:{}{}{}{}{}",
        separator,
        hour,
        minute < 10 ? "0" : "",
        minute,
        second < 10 ? "0" : "",
        second,
        has_subseconds ? "." : "",
        has_subseconds ? std::to_string(subseconds(generator)) : "",
        separator);
}

String RandomGenerator::nextDateTime(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 2))
        {
            case 0:
                return separator + "1970-01-01 00:00:00" + separator; /// Epoch / min DateTime
            case 1:
                return separator + "2106-02-07 06:28:15" + separator; /// Max DateTime (2^32-1 seconds)
            case 2:
                return separator + "1970-01-01 00:00:01" + separator; /// One second after epoch
            default:
                UNREACHABLE();
        }
    }
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    const uint32_t hour = hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);
    return fmt::format(
        "{}{}-{}{}-{}{} {}{}:{}{}:{}{}{}{}{}",
        separator,
        1970 + datetime_years(generator),
        month < 10 ? "0" : "",
        month,
        day < 10 ? "0" : "",
        day,
        hour < 10 ? "0" : "",
        hour,
        minute < 10 ? "0" : "",
        minute,
        second < 10 ? "0" : "",
        second,
        has_subseconds ? "." : "",
        has_subseconds ? std::to_string(subseconds(generator)) : "",
        separator);
}

String RandomGenerator::nextDateTime64(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    if (this->nextMediumNumber() < 16)
    {
        const String sub = has_subseconds ? ".999999999" : "";
        const String sub0 = has_subseconds ? ".000000000" : "";
        switch (this->randomInt<uint32_t>(0, 2))
        {
            case 0:
                return separator + "1900-01-01 00:00:00" + sub0 + separator; /// Min DateTime64
            case 1:
                return separator + "2299-12-31 23:59:59" + sub + separator; /// Max DateTime64
            case 2:
                return separator + "1970-01-01 00:00:00" + sub0 + separator; /// Epoch
            default:
                UNREACHABLE();
        }
    }
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    const uint32_t hour = hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);
    return fmt::format(
        "{}{}-{}{}-{}{} {}{}:{}{}:{}{}{}{}{}",
        separator,
        1900 + datetime64_years(generator),
        month < 10 ? "0" : "",
        month,
        day < 10 ? "0" : "",
        day,
        hour < 10 ? "0" : "",
        hour,
        minute < 10 ? "0" : "",
        minute,
        second < 10 ? "0" : "",
        second,
        has_subseconds ? "." : "",
        has_subseconds ? std::to_string(subseconds(generator)) : "",
        separator);
}

double RandomGenerator::randomGauss(const double mean, const double stddev)
{
    std::normal_distribution<double> d{mean, stddev};
    return d(generator);
}

double RandomGenerator::randomZeroOne()
{
    return zero_one(generator);
}

String RandomGenerator::nextJSONCol()
{
    return pickRandomly(jcols);
}

String RandomGenerator::nextTokenString()
{
    return pickRandomly(this->nextSmallNumber() < 3 ? nasty_strings : (this->nextBool() ? common_english : common_chinese));
}

String RandomGenerator::nextString(const String & delimiter, const bool allow_nasty, const uint32_t limit)
{
    String ret;
    bool use_bad_utf8 = false;

    if (delimiter == "'" && this->nextMediumNumber() < 4)
    {
        ret += "x";
        use_bad_utf8 = true;
    }
    ret += delimiter;
    /* A few times generate empty strings */
    if (this->nextMediumNumber() > 2)
    {
        /// ~3% chance: repeated single character (stresses compression, string functions like repeat/position/like)
        if (!use_bad_utf8 && this->nextMediumNumber() < 4)
        {
            static const std::vector<char> repeat_chars = {'a', '0', ' ', '\t', '%', '_', '\\', '"', '/', '-'};
            char c = this->pickRandomly(repeat_chars);

            if (delimiter.size() == 1 && c == delimiter[0])
                c = delimiter[0] == 'a' ? 'b' : 'a';
            ret += String(this->randomInt<uint32_t>(0, std::min(limit, UINT32_C(65536))), c);
        }
        else
        {
            const String & pick = pickRandomly(
                use_bad_utf8
                    ? bad_utf8
                    : (allow_nasty && this->nextSmallNumber() < 3 ? nasty_strings : (this->nextBool() ? common_english : common_chinese)));
            if ((pick.length() >> (use_bad_utf8 ? 1 : 0)) < limit)
            {
                ret += pick;
                /// A few times, generate a large string
                if (this->nextMediumNumber() < 16)
                {
                    uint32_t i = 0;
                    uint32_t len = static_cast<uint32_t>(pick.size());
                    const bool use_space = this->nextBool();
                    const uint32_t max_iterations = this->nextBool() ? 10000 : this->nextMediumNumber();

                    while (i < max_iterations)
                    {
                        const String & npick = pickRandomly(
                            use_bad_utf8
                                ? bad_utf8
                                : (allow_nasty && this->nextSmallNumber() < 3 ? nasty_strings
                                                                              : (this->nextBool() ? common_english : common_chinese)));

                        len += ((use_space ? 1 : 0) + (npick.length() >> (use_bad_utf8 ? 1 : 0)));
                        if (len < limit)
                        {
                            ret += use_space ? " " : "";
                            ret += npick;
                        }
                        else
                        {
                            break;
                        }
                        i++;
                    }
                }
            }
            else
            {
                ret += use_bad_utf8 ? "00" : "a";
            }
        }
    }
    ret += delimiter;
    return ret;
}

static const constexpr char hexDigits[] = "0123456789abcdef";

String RandomGenerator::nextUUID()
{
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 2))
        {
            case 0:
                return "00000000-0000-0000-0000-000000000000"; /// Nil UUID
            case 1:
                return "ffffffff-ffff-ffff-ffff-ffffffffffff"; /// Max UUID
            case 2:
                return "00000000-0000-0000-0000-000000000001"; /// Near-nil
            default:
                UNREACHABLE();
        }
    }
    return fmt::format(
        "{}{}{}{}{}{}{}{}-{}{}{}{}-{}{}{}{}-{}{}{}{}-{}{}{}{}{}{}{}{}{}{}{}{}",
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)]);
}

String RandomGenerator::nextIPv4()
{
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 4))
        {
            case 0:
                return "0.0.0.0"; /// Unspecified
            case 1:
                return "127.0.0.1"; /// Loopback
            case 2:
                return "255.255.255.255"; /// Broadcast / max
            case 3:
                return "192.168.1.1"; /// Private (class C)
            case 4:
                return "10.0.0.1"; /// Private (class A)
            default:
                UNREACHABLE();
        }
    }
    return fmt::format("{}.{}.{}.{}", this->nextRandomUInt8(), this->nextRandomUInt8(), this->nextRandomUInt8(), this->nextRandomUInt8());
}

String RandomGenerator::nextIPv6()
{
    if (this->nextMediumNumber() < 16)
    {
        switch (this->randomInt<uint32_t>(0, 4))
        {
            case 0:
                return "::"; /// Unspecified
            case 1:
                return "::1"; /// Loopback
            case 2:
                return "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"; /// Max
            case 3:
                return "fe80::1"; /// Link-local
            case 4:
                return "::ffff:127.0.0.1"; /// IPv4-mapped loopback
            default:
                UNREACHABLE();
        }
    }
    return fmt::format(
        "{}{}{}{}:{}{}{}{}:{}{}{}{}:{}{}{}{}:{}{}{}{}:{}{}{}{}:{}{}{}{}:{}{}{}{}",
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)],
        hexDigits[hex_digits_dist(generator)]);
}

void RandomGenerator::pickWeighted(std::initializer_list<std::pair<uint32_t, std::function<void()>>> options)
{
    uint32_t prob_space = 0;
    for (const auto & [w, f] : options)
        prob_space += w;
    chassert(prob_space > 0, "At least one option must have a non-zero weight");
    std::uniform_int_distribution<uint32_t> dist(1, prob_space);
    const uint32_t nopt = dist(generator);
    uint32_t cumulative = 0;
    for (const auto & [w, f] : options)
    {
        cumulative += w;
        if (w != 0 && nopt <= cumulative)
        {
            f();
            return;
        }
    }
    UNREACHABLE();
}

}
