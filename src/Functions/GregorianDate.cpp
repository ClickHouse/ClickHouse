#include <Functions/GregorianDate.h>

#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_FORMAT_DATETIME;
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr bool is_leap_year(int32_t year)
    {
        return (year % 4 == 0) && ((year % 400 == 0) || (year % 100 != 0));
    }

    constexpr uint8_t monthLength(bool is_leap_year, uint8_t month)
    {
        switch (month)
        {
        case  1: return 31;
        case  2: return is_leap_year ? 29 : 28;
        case  3: return 31;
        case  4: return 30;
        case  5: return 31;
        case  6: return 30;
        case  7: return 31;
        case  8: return 31;
        case  9: return 30;
        case 10: return 31;
        case 11: return 30;
        case 12: return 31;
        default:
            std::terminate();
        }
    }

    /** Integer division truncated toward negative infinity.
      */
    template <typename I, typename J>
    constexpr I div(I x, J y)
    {
        const auto y_cast = static_cast<I>(y);
        if (x > 0 && y_cast < 0)
            return ((x - 1) / y_cast) - 1;
        if (x < 0 && y_cast > 0)
            return ((x + 1) / y_cast) - 1;
        return x / y_cast;
    }

    /** Integer modulus, satisfying div(x, y)*y + mod(x, y) == x.
      */
    template <typename I, typename J>
    constexpr I mod(I x, J y)
    {
        const auto y_cast = static_cast<I>(y);
        const auto r = x % y_cast;
        if ((x > 0 && y_cast < 0) || (x < 0 && y_cast > 0))
            return r == 0 ? static_cast<I>(0) : r + y_cast;
        return r;
    }

    /** Like std::min(), but the type of operands may differ.
      */
    template <typename I, typename J>
    constexpr I min(I x, J y)
    {
        const auto y_cast = static_cast<I>(y);
        return x < y_cast ? x : y_cast;
    }

    inline char readDigit(ReadBuffer & in)
    {
        char c;
        if (!in.read(c))
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot parse input: expected a digit at the end of stream");
        if (c < '0' || c > '9')
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot read input: expected a digit but got something else");
        return c - '0';
    }

    inline bool tryReadDigit(ReadBuffer & in, char & c)
    {
        if (in.read(c) && c >= '0' && c <= '9')
        {
            c -= '0';
            return true;
        }

        return false;
    }
}

void GregorianDate::init(ReadBuffer & in)
{
    year_ = readDigit(in) * 1000
          + readDigit(in) * 100
          + readDigit(in) * 10
          + readDigit(in);

    assertChar('-', in);

    month_ = readDigit(in) * 10
           + readDigit(in);

    assertChar('-', in);

    day_of_month_ = readDigit(in) * 10
                + readDigit(in);

    assertEOF(in);

    if (month_ < 1 || month_ > 12 || day_of_month_ < 1 || day_of_month_ > monthLength(is_leap_year(year_), month_))
        throw Exception(ErrorCodes::CANNOT_PARSE_DATE, "Invalid date, out of range (year: {}, month: {}, day_of_month: {}).", year_, month_, day_of_month_);
}

bool GregorianDate::tryInit(ReadBuffer & in)
{
    char c[8];

    if (   !tryReadDigit(in, c[0])
        || !tryReadDigit(in, c[1])
        || !tryReadDigit(in, c[2])
        || !tryReadDigit(in, c[3])
        || !checkChar('-', in)
        || !tryReadDigit(in, c[4])
        || !tryReadDigit(in, c[5])
        || !checkChar('-', in)
        || !tryReadDigit(in, c[6])
        || !tryReadDigit(in, c[7])
        || !in.eof())
    {
        return false;
    }

    year_ = c[0] * 1000 + c[1] * 100 + c[2] * 10 + c[3];
    month_ = c[4] * 10 + c[5];
    day_of_month_ = c[6] * 10 + c[7];

    if (month_ < 1 || month_ > 12 || day_of_month_ < 1 || day_of_month_ > monthLength(is_leap_year(year_), month_))
        return false;

    return true;
}

GregorianDate::GregorianDate(ReadBuffer & in)
{
    init(in);
}

void GregorianDate::init(int64_t modified_julian_day)
{
    const OrdinalDate ord(modified_julian_day);
    const MonthDay md(is_leap_year(ord.year()), ord.dayOfYear());

    year_  = ord.year();
    month_ = md.month();
    day_of_month_ = md.dayOfMonth();
}

bool GregorianDate::tryInit(int64_t modified_julian_day)
{
    OrdinalDate ord;
    if (!ord.tryInit(modified_julian_day))
        return false;

    MonthDay md(is_leap_year(ord.year()), ord.dayOfYear());

    year_  = ord.year();
    month_ = md.month();
    day_of_month_ = md.dayOfMonth();

    return true;
}

GregorianDate::GregorianDate(int64_t modified_julian_day)
{
    init(modified_julian_day);
}

int64_t GregorianDate::toModifiedJulianDay() const
{
    const MonthDay md(month_, day_of_month_);

    const auto day_of_year = md.dayOfYear(is_leap_year(year_));

    const OrdinalDate ord(year_, day_of_year);
    return ord.toModifiedJulianDay();
}

bool GregorianDate::tryToModifiedJulianDay(int64_t & res) const
{
    const MonthDay md(month_, day_of_month_);
    const auto day_of_year = md.dayOfYear(is_leap_year(year_));
    OrdinalDate ord;

    if (!ord.tryInit(year_, day_of_year))
        return false;

    res = ord.toModifiedJulianDay();
    return true;
}

template <typename ReturnType>
ReturnType GregorianDate::writeImpl(WriteBuffer & buf) const
{
    if (year_ < 0 || year_ > 9999)
    {
        if constexpr (std::is_same_v<ReturnType, void>)
            throw Exception(ErrorCodes::CANNOT_FORMAT_DATETIME,
                "Impossible to stringify: year too big or small: {}", year_);
        else
            return false;
    }
    else
    {
        auto y = year_;
        writeChar('0' + y / 1000, buf); y %= 1000;
        writeChar('0' + y /  100, buf); y %=  100;
        writeChar('0' + y /   10, buf); y %=   10;
        writeChar('0' + y       , buf);

        writeChar('-', buf);

        auto m = month_;
        writeChar('0' + m / 10, buf); m %= 10;
        writeChar('0' + m     , buf);

        writeChar('-', buf);

        auto d = day_of_month_;
        writeChar('0' + d / 10, buf); d %= 10;
        writeChar('0' + d     , buf);
    }

    return ReturnType(true);
}

std::string GregorianDate::toString() const
{
    WriteBufferFromOwnString buf;
    write(buf);
    return buf.str();
}

void OrdinalDate::init(int32_t year, uint16_t day_of_year)
{
    year_ = year;
    day_of_year_ = day_of_year;

    if (day_of_year < 1 || day_of_year > (is_leap_year(year) ? 366 : 365))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid ordinal date: {}-{}", year, day_of_year);
}

bool OrdinalDate::tryInit(int32_t year, uint16_t day_of_year)
{
    year_ = year;
    day_of_year_ = day_of_year;

    return !(day_of_year < 1 || day_of_year > (is_leap_year(year) ? 366 : 365));
}

void OrdinalDate::init(int64_t modified_julian_day)
{
    if (!tryInit(modified_julian_day))
        throw Exception(
            ErrorCodes::CANNOT_FORMAT_DATETIME,
            "Value cannot be represented as date because it's out of range");
}

bool OrdinalDate::tryInit(int64_t modified_julian_day)
{
    /// This function supports day number from -678941 to 2973483 (which represent 0000-01-01 and 9999-12-31 respectively).

    if (modified_julian_day < -678941)
        return false;

    if (modified_julian_day > 2973483)
        return false;

    const auto a         = modified_julian_day + 678575;
    const auto quad_cent = div(a, 146097);
    const auto b         = mod(a, 146097);
    const auto cent      = min(div(b, 36524), 3);
    const auto c         = b - cent * 36524;
    const auto quad      = div(c, 1461);
    const auto d         = mod(c, 1461);
    const auto y         = min(div(d, 365), 3);

    day_of_year_ = d - y * 365 + 1;
    year_ = static_cast<int32_t>(quad_cent * 400 + cent * 100 + quad * 4 + y + 1);

    return true;
}


OrdinalDate::OrdinalDate(int32_t year, uint16_t day_of_year)
{
    init(year, day_of_year);
}

OrdinalDate::OrdinalDate(int64_t modified_julian_day)
{
    init(modified_julian_day);
}

int64_t OrdinalDate::toModifiedJulianDay() const noexcept
{
    const auto y = year_ - 1;

    return day_of_year_
        + 365 * y
        + div(y, 4)
        - div(y, 100)
        + div(y, 400)
        - 678576;
}

MonthDay::MonthDay(uint8_t month, uint8_t day_of_month)
    : month_(month)
    , day_of_month_(day_of_month)
{
    if (month < 1 || month > 12)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid month: {}", month);
    /* We can't validate day_of_month here, because we don't know if
     * it's a leap year. */
}

MonthDay::MonthDay(bool is_leap_year, uint16_t day_of_year)
{
    if (day_of_year < 1 || day_of_year > (is_leap_year ? 366 : 365))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of year: {}{}",
                        (is_leap_year ? "leap, " : "non-leap, "), day_of_year);

    month_ = 1;
    uint16_t d = day_of_year;
    while (true)
    {
        const auto len = monthLength(is_leap_year, month_);
        if (d <= len)
            break;
        ++month_;
        d -= len;
    }
    day_of_month_ = d;
}

uint16_t MonthDay::dayOfYear(bool is_leap_year) const
{
    if (day_of_month_ < 1 || day_of_month_ > monthLength(is_leap_year, month_))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid day of month: {}{}-{}",
            (is_leap_year ? "leap, " : "non-leap, "), month_, day_of_month_);
    }
    const auto k = month_ <= 2 ? 0 : is_leap_year ? -1 :-2;
    return (367 * month_ - 362) / 12 + k + day_of_month_;
}

template void GregorianDate::writeImpl<void>(WriteBuffer & buf) const;
template bool GregorianDate::writeImpl<bool>(WriteBuffer & buf) const;

}
