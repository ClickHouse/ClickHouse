#include <Client/BuzzHouse/Generator/RandomGenerator.h>

namespace BuzzHouse
{

uint32_t RandomGenerator::getSeed() const
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

char RandomGenerator::nextDigit()
{
    return static_cast<char>(digits(generator));
}

bool RandomGenerator::nextBool()
{
    return dist4(generator) == 2;
}

//range [1970-01-01, 2149-06-06]
void RandomGenerator::nextDate(std::string & ret)
{
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);

    ret += std::to_string(1970 + date_years(generator));
    ret += "-";
    if (month < 10)
    {
        ret += "0";
    }
    ret += std::to_string(month);
    ret += "-";
    if (day < 10)
    {
        ret += "0";
    }
    ret += std::to_string(day);
}

//range [1900-01-01, 2299-12-31]
void RandomGenerator::nextDate32(std::string & ret)
{
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);

    ret += std::to_string(1900 + datetime64_years(generator));
    ret += "-";
    if (month < 10)
    {
        ret += "0";
    }
    ret += std::to_string(month);
    ret += "-";
    if (day < 10)
    {
        ret += "0";
    }
    ret += std::to_string(day);
}

//range [1970-01-01 00:00:00, 2106-02-07 06:28:15]
void RandomGenerator::nextDateTime(std::string & ret)
{
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    const uint32_t hour = hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);

    ret += std::to_string(1970 + datetime_years(generator));
    ret += "-";
    if (month < 10)
    {
        ret += "0";
    }
    ret += std::to_string(month);
    ret += "-";
    if (day < 10)
    {
        ret += "0";
    }
    ret += std::to_string(day);
    ret += " ";
    if (hour < 10)
    {
        ret += "0";
    }
    ret += std::to_string(hour);
    ret += ":";
    if (minute < 10)
    {
        ret += "0";
    }
    ret += std::to_string(minute);
    ret += ":";
    if (second < 10)
    {
        ret += "0";
    }
    ret += std::to_string(second);
}

//range [1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]
void RandomGenerator::nextDateTime64(std::string & ret)
{
    const uint32_t month = months(generator);
    const uint32_t day = days[month - 1](generator);
    const uint32_t hour = hours(generator);
    const uint32_t minute = minutes(generator);
    const uint32_t second = minutes(generator);

    ret += std::to_string(1900 + datetime64_years(generator));
    ret += "-";
    if (month < 10)
    {
        ret += "0";
    }
    ret += std::to_string(month);
    ret += "-";
    if (day < 10)
    {
        ret += "0";
    }
    ret += std::to_string(day);
    ret += " ";
    if (hour < 10)
    {
        ret += "0";
    }
    ret += std::to_string(hour);
    ret += ":";
    if (minute < 10)
    {
        ret += "0";
    }
    ret += std::to_string(minute);
    ret += ":";
    if (second < 10)
    {
        ret += "0";
    }
    ret += std::to_string(second);
}

double RandomGenerator::randomGauss(const double mean, const double stddev)
{
    std::normal_distribution d{mean, stddev};
    return d(generator);
}

double RandomGenerator::randomZeroOne()
{
    return zero_one(generator);
}

void RandomGenerator::nextJSONCol(std::string & ret)
{
    const std::string & pick = pickRandomlyFromVector(jcols);

    ret += pick;
}

void RandomGenerator::nextString(std::string & ret, const std::string & delimiter, const bool allow_nasty, const uint32_t limit)
{
    bool use_bad_utf8 = false;

    if (delimiter == "'" && this->nextMediumNumber() < 4)
    {
        ret += "x";
        use_bad_utf8 = true;
    }
    ret += delimiter;
    const std::string & pick = pickRandomlyFromVector(
        use_bad_utf8 ? bad_utf8
                     : (allow_nasty && this->nextSmallNumber() < 3 ? nasty_strings : (this->nextBool() ? common_english : common_chinese)));

    if ((pick.length() >> (use_bad_utf8 ? 1 : 0)) < limit)
    {
        ret += pick;
        /* A few times, generate a large string */
        if (this->nextLargeNumber() < 4)
        {
            uint32_t i = 0;
            uint32_t len = static_cast<uint32_t>(pick.size());
            const uint32_t max_iterations = this->nextBool() ? 10000 : this->nextMediumNumber();

            while (i < max_iterations)
            {
                const std::string & npick = pickRandomlyFromVector(
                    use_bad_utf8 ? bad_utf8
                                 : (allow_nasty && this->nextSmallNumber() < 3 ? nasty_strings
                                                                               : (this->nextBool() ? common_english : common_chinese)));

                len += (npick.length() >> (use_bad_utf8 ? 1 : 0));
                if (len < limit)
                {
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
    ret += delimiter;
}

static const constexpr char hex_digits[] = "0123456789abcdef";

void RandomGenerator::nextUUID(std::string & ret)
{
    for (uint32_t i = 0; i < 8; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
    }
    ret += "-";
    for (uint32_t i = 0; i < 4; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
    }
    ret += "-";
    for (uint32_t i = 0; i < 4; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
    }
    ret += "-";
    for (uint32_t i = 0; i < 4; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
    }
    ret += "-";
    for (uint32_t i = 0; i < 12; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
    }
}

void RandomGenerator::nextIPv4(std::string & ret)
{
    ret += std::to_string(this->nextRandomUInt8());
    ret += ".";
    ret += std::to_string(this->nextRandomUInt8());
    ret += ".";
    ret += std::to_string(this->nextRandomUInt8());
    ret += ".";
    ret += std::to_string(this->nextRandomUInt8());
}

void RandomGenerator::nextIPv6(std::string & ret)
{
    for (uint32_t i = 0; i < 8; i++)
    {
        ret += hex_digits[hex_digits_dist(generator)];
        if (i < 7)
        {
            ret += ":";
        }
    }
}

}
