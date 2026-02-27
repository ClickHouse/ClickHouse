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
    else
    {
        const uint32_t month = months(generator);
        const uint32_t day = days[month - 1](generator);

        return fmt::format(
            "{}{}-{}{}-{}{}{}", separator, 1970 + date_years(generator), month < 10 ? "0" : "", month, day < 10 ? "0" : "", day, separator);
    }
}

String RandomGenerator::nextDate32(const String & separator, const bool allow_func)
{
    if (allow_func && this->nextMediumNumber() < 16)
    {
        return "today()";
    }
    else
    {
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
}

String RandomGenerator::nextTime(const String & separator, const bool allow_func)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    else
    {
        const int32_t hour = time_hours(generator);
        const uint32_t minute = minutes(generator);
        const uint32_t second = minutes(generator);

        return fmt::format("{}{}:{}{}:{}{}{}", separator, hour, minute < 10 ? "0" : "", minute, second < 10 ? "0" : "", second, separator);
    }
}

String RandomGenerator::nextTime64(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    else
    {
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
}

String RandomGenerator::nextDateTime(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    else
    {
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
}

String RandomGenerator::nextDateTime64(const String & separator, const bool allow_func, const bool has_subseconds)
{
    if (allow_func && this->nextMediumNumber() < 21)
    {
        const int32_t offset_seconds = second_offsets(generator);

        return fmt::format("addSeconds(now(), {})", offset_seconds);
    }
    else
    {
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
                        use_bad_utf8 ? bad_utf8
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
            ret += "a";
        }
    }
    ret += delimiter;
    return ret;
}

static const constexpr char hexDigits[] = "0123456789abcdef";

String RandomGenerator::nextUUID()
{
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
    return fmt::format("{}.{}.{}.{}", this->nextRandomUInt8(), this->nextRandomUInt8(), this->nextRandomUInt8(), this->nextRandomUInt8());
}

String RandomGenerator::nextIPv6()
{
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

}
