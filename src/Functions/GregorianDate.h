#pragma once

#include <Common/Exception.h>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <cstdint>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
        extern const int CANNOT_PARSE_DATE;
        extern const int CANNOT_FORMAT_DATETIME;
        extern const int LOGICAL_ERROR;
    }

    /** Proleptic Gregorian calendar date. YearT is an integral type
      * which should be at least 32 bits wide, and should preferably
      * be signed.
     */
    template <typename YearT = int32_t>
    class GregorianDate
    {
    public:
        GregorianDate() = delete;

        /** Construct from date in text form 'YYYY-MM-DD' by reading from
          * ReadBuffer.
          */
        GregorianDate(ReadBuffer & in);

        /** Construct from Modified Julian Day. The type T is an
          * integral type which should be at least 32 bits wide, and
          * should preferably signed.
          */
        template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> * = nullptr>
        GregorianDate(T mjd);

        /** Convert to Modified Julian Day. The type T is an integral type
          * which should be at least 32 bits wide, and should preferably
          * signed.
          */
        template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> * = nullptr>
        T toMJD() const;

        /** Write the date in text form 'YYYY-MM-DD' to a buffer.
          */
        void write(WriteBuffer & buf) const;

        /** Convert to a string in text form 'YYYY-MM-DD'.
          */
        std::string toString() const;

        YearT year() const noexcept
        {
            return year_;
        }

        uint8_t month() const noexcept
        {
            return month_;
        }

        uint8_t dayOfMonth() const noexcept
        {
            return dayOfMonth_;
        }

    private:
        YearT year_;
        uint8_t month_;
        uint8_t dayOfMonth_;
    };

    /** ISO 8601 Ordinal Date. YearT is an integral type which should
      * be at least 32 bits wide, and should preferably signed.
     */
    template <typename YearT = int32_t>
    class OrdinalDate
    {
    public:
        OrdinalDate(YearT year, uint16_t dayOfYear);

        /** Construct from Modified Julian Day. The type T is an
          * integral type which should be at least 32 bits wide, and
          * should preferably signed.
          */
        template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> * = nullptr>
        OrdinalDate(T mjd);

        /** Convert to Modified Julian Day. The type T is an integral
          * type which should be at least 32 bits wide, and should
          * preferably be signed.
          */
        template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> * = nullptr>
        T toMJD() const noexcept;

        YearT year() const noexcept
        {
            return year_;
        }

        uint16_t dayOfYear() const noexcept
        {
            return dayOfYear_;
        }

    private:
        YearT year_;
        uint16_t dayOfYear_;
    };

    class MonthDay
    {
    public:
        /** Construct from month and day. */
        MonthDay(uint8_t month, uint8_t dayOfMonth);

        /** Construct from day of year in Gregorian or Julian
          * calendars to month and day.
          */
        MonthDay(bool isLeapYear, uint16_t dayOfYear);

        /** Convert month and day in Gregorian or Julian calendars to
          * day of year.
          */
        uint16_t dayOfYear(bool isLeapYear) const;

        uint8_t month() const noexcept
        {
            return month_;
        }

        uint8_t dayOfMonth() const noexcept
        {
            return dayOfMonth_;
        }

    private:
        uint8_t month_;
        uint8_t dayOfMonth_;
    };
}

/* Implementation */

namespace gd {
    using namespace DB;

    template <typename YearT>
    static inline constexpr bool isLeapYear(YearT year)
    {
        return ( (year % 4 == 0)
                 &&
                 ( (year % 400 == 0)
                   ||
                   (! (year % 100 == 0)) ) );
    }

    static inline constexpr uint8_t monthLength(bool isLeapYear, uint8_t month)
    {
        switch (month)
        {
        case  1: return 31;
        case  2: return isLeapYear ? 29 : 28;
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
    static inline constexpr I div(I x, J y)
    {
        const auto y_ = static_cast<I>(y);
        if (x > 0 && y_ < 0) {
            return ((x - 1) / y_) - 1;
        }
        else if (x < 0 && y_ > 0) {
            return ((x + 1) / y_) - 1;
        }
        else {
            return x / y_;
        }
    }

    /** Integer modulus, satisfying div(x, y)*y + mod(x, y) == x.
      */
    template <typename I, typename J>
    static inline constexpr I mod(I x, J y)
    {
        const auto y_ = static_cast<I>(y);
        const auto r = x % y_;
        if ((x > 0 && y_ < 0) || (x < 0 && y_ > 0)) {
            return r == 0 ? static_cast<I>(0) : r + y_;
        }
        else {
            return r;
        }
    }

    /** Like std::min(), but the type of operands may differ.
      */
    template <typename I, typename J>
    static inline constexpr I min(I x, J y)
    {
        const auto y_ = static_cast<I>(y);
        return x < y_ ? x : y_;
    }

    static inline char readDigit(ReadBuffer & in)
    {
        char c;
        if (!in.read(c)) {
            throw Exception(
                "Cannot parse input: expected a digit at the end of stream",
                ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
        }
        else if (c < '0' || c > '9') {
            throw Exception(
                "Cannot read input: expected a digit but got something else",
                ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
        }
        else {
            return c - '0';
        }
    }
}

namespace DB
{
    template <typename YearT>
    GregorianDate<YearT>::GregorianDate(ReadBuffer & in)
    {
        year_ = gd::readDigit(in) * 1000
              + gd::readDigit(in) *  100
              + gd::readDigit(in) *   10
              + gd::readDigit(in);

        assertChar('-', in);

        month_ = gd::readDigit(in) * 10
               + gd::readDigit(in);

        assertChar('-', in);

        dayOfMonth_ = gd::readDigit(in) * 10
                    + gd::readDigit(in);

        assertEOF(in);

        if (month_      < 1 || month_      > 12 ||
            dayOfMonth_ < 1 || dayOfMonth_ > gd::monthLength(gd::isLeapYear(year_), month_))
        {
            throw Exception(
                "Invalid date: " + toString(), ErrorCodes::CANNOT_PARSE_DATE);
        }
    }

    template <typename YearT>
    template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> *>
    GregorianDate<YearT>::GregorianDate(T mjd)
    {
        const OrdinalDate<YearT> ord(mjd);
        const MonthDay md(gd::isLeapYear(ord.year()), ord.dayOfYear());
        year_       = ord.year();
        month_      = md.month();
        dayOfMonth_ = md.dayOfMonth();
    }

    template <typename YearT>
    template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> *>
    T GregorianDate<YearT>::toMJD() const
    {
        const MonthDay md(month_, dayOfMonth_);
        const auto dayOfYear = md.dayOfYear(gd::isLeapYear(year_));
        const OrdinalDate<YearT> ord(year_, dayOfYear);
        return ord.template toMJD<T>();
    }

    template <typename YearT>
    void GregorianDate<YearT>::write(WriteBuffer & buf) const
    {
        if (year_ < 0 || year_ > 9999)
        {
            throw Exception(
                "Impossible to stringify: year too big or small: " + DB::toString(year_),
                ErrorCodes::CANNOT_FORMAT_DATETIME);
        }
        else {
            auto y = year_;
            writeChar('0' + y / 1000, buf); y %= 1000;
            writeChar('0' + y /  100, buf); y %=  100;
            writeChar('0' + y /   10, buf); y %=   10;
            writeChar('0' + y       , buf);;

            writeChar('-', buf);

            auto m = month_;
            writeChar('0' + m / 10, buf); m %= 10;
            writeChar('0' + m     , buf);

            writeChar('-', buf);

            auto d = dayOfMonth_;
            writeChar('0' + d / 10, buf); d %= 10;
            writeChar('0' + d     , buf);
        }
    }

    template <typename YearT>
    std::string GregorianDate<YearT>::toString() const
    {
        WriteBufferFromOwnString buf;
        write(buf);
        return buf.str();
    }

    template <typename YearT>
    OrdinalDate<YearT>::OrdinalDate(YearT year, uint16_t dayOfYear)
        : year_(year)
        , dayOfYear_(dayOfYear)
    {
        if (dayOfYear < 1 || dayOfYear > (gd::isLeapYear(year) ? 366 : 365))
        {
            throw Exception(
                "Invalid ordinal date: " + toString(year) + "-" + toString(dayOfYear),
                ErrorCodes::LOGICAL_ERROR);
        }
    }

    template <typename YearT>
    template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> *>
    OrdinalDate<YearT>::OrdinalDate(T mjd)
    {
        const auto a         = mjd + 678575;
        const auto quad_cent = gd::div(a, 146097);
        const auto b         = gd::mod(a, 146097);
        const auto cent      = gd::min(gd::div(b, 36524), 3);
        const auto c         = b - cent * 36524;
        const auto quad      = gd::div(c, 1461);
        const auto d         = gd::mod(c, 1461);
        const auto y         = gd::min(gd::div(d, 365), 3);
        dayOfYear_ = d - y * 365 + 1;
        year_      = quad_cent * 400 + cent * 100 + quad * 4 + y + 1;
    }

    template <typename YearT>
    template <typename T, std::enable_if_t<wide::IntegralConcept<T>()> *>
    T OrdinalDate<YearT>::toMJD() const noexcept
    {
        const auto y = year_ - 1;
        return dayOfYear_
            +  365 * y
            +  gd::div(y,   4)
            -  gd::div(y, 100)
            +  gd::div(y, 400)
            -  678576;
    }

    inline MonthDay::MonthDay(uint8_t month, uint8_t dayOfMonth)
        : month_(month)
        , dayOfMonth_(dayOfMonth)
    {
        if (month < 1 || month > 12) {
            throw Exception(
                "Invalid month: " + DB::toString(month),
                ErrorCodes::LOGICAL_ERROR);
        }
        /* We can't validate dayOfMonth here, because we don't know if
         * it's a leap year. */
    }

    inline MonthDay::MonthDay(bool isLeapYear, uint16_t dayOfYear)
    {
        if (dayOfYear < 1 || dayOfYear > (isLeapYear ? 366 : 365))
        {
            throw Exception(
                std::string("Invalid day of year: ") +
                (isLeapYear ? "leap, " : "non-leap, ") + DB::toString(dayOfYear),
                ErrorCodes::LOGICAL_ERROR);
        }

        month_ = 1;
        uint16_t d = dayOfYear;
        while (true) {
            const auto len = gd::monthLength(isLeapYear, month_);
            if (d > len)
            {
                month_++;
                d -= len;
            }
            else {
                break;
            }
        }
        dayOfMonth_ = d;
    }

    inline uint16_t MonthDay::dayOfYear(bool isLeapYear) const
    {
        if (dayOfMonth_ < 1 || dayOfMonth_ > gd::monthLength(isLeapYear, month_))
        {
            throw Exception(
                std::string("Invalid day of month: ") +
                (isLeapYear ? "leap, " : "non-leap, ") + DB::toString(month_) +
                "-" + DB::toString(dayOfMonth_),
                ErrorCodes::LOGICAL_ERROR);
        }
        const auto k = month_ <= 2 ?  0
                     : isLeapYear  ? -1
                     :               -2;
        return (367 * month_ - 362) / 12 + k + dayOfMonth_;
    }
}
