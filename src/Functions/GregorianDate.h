#pragma once

#include <Core/Types.h>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Proleptic Gregorian calendar date.
class GregorianDate
{
public:
    GregorianDate() = default;

    void init(ReadBuffer & in);
    bool tryInit(ReadBuffer & in);

    /** Construct from date in text form 'YYYY-MM-DD' by reading from
      * ReadBuffer.
      */
    explicit GregorianDate(ReadBuffer & in);

    void init(int64_t modified_julian_day);
    bool tryInit(int64_t modified_julian_day);

    /** Construct from Modified Julian Day. The type T is an
      * integral type which should be at least 32 bits wide, and
      * should preferably signed.
      */
    explicit GregorianDate(int64_t modified_julian_day);

    /** Convert to Modified Julian Day. The type T is an integral type
      * which should be at least 32 bits wide, and should preferably
      * signed.
      */
    int64_t toModifiedJulianDay() const;
    bool tryToModifiedJulianDay(int64_t & res) const;

    /** Write the date in text form 'YYYY-MM-DD' to a buffer.
      */
    void write(WriteBuffer & buf) const
    {
        writeImpl<void>(buf);
    }

    bool tryWrite(WriteBuffer & buf) const
    {
        return writeImpl<bool>(buf);
    }

    /** Convert to a string in text form 'YYYY-MM-DD'.
      */
    std::string toString() const;

    int32_t year() const noexcept
    {
        return year_;
    }

    uint8_t month() const noexcept
    {
        return month_;
    }

    uint8_t dayOfMonth() const noexcept
    {
        return day_of_month_;
    }

private:
    int32_t year_ = 0;
    uint8_t month_ = 0;
    uint8_t day_of_month_ = 0;

    template <typename ReturnType>
    ReturnType writeImpl(WriteBuffer & buf) const;
};

/** ISO 8601 Ordinal Date.
 */
class OrdinalDate
{
public:
    OrdinalDate() = default;

    void init(int32_t year, uint16_t day_of_year);
    bool tryInit(int32_t year, uint16_t day_of_year);

    void init(int64_t modified_julian_day);
    bool tryInit(int64_t modified_julian_day);

    OrdinalDate(int32_t year, uint16_t day_of_year);

    /** Construct from Modified Julian Day. The type T is an
      * integral type which should be at least 32 bits wide, and
      * should preferably signed.
      */
    explicit OrdinalDate(int64_t modified_julian_day);

    /** Convert to Modified Julian Day. The type T is an integral
      * type which should be at least 32 bits wide, and should
      * preferably be signed.
      */
    int64_t toModifiedJulianDay() const noexcept;

    int32_t year() const noexcept
    {
        return year_;
    }

    uint16_t dayOfYear() const noexcept
    {
        return day_of_year_;
    }

private:
    int32_t year_ = 0;
    uint16_t day_of_year_ = 0;
};

class MonthDay
{
public:
    /** Construct from month and day. */
    MonthDay(uint8_t month, uint8_t day_of_month);

    /** Construct from day of year in Gregorian or Julian
      * calendars to month and day.
      */
    MonthDay(bool is_leap_year, uint16_t day_of_year);

    /** Convert month and day in Gregorian or Julian calendars to
      * day of year.
      */
    uint16_t dayOfYear(bool is_leap_year) const;

    uint8_t month() const noexcept
    {
        return month_;
    }

    uint8_t dayOfMonth() const noexcept
    {
        return day_of_month_;
    }

private:
    uint8_t month_ = 0;
    uint8_t day_of_month_ = 0;
};

}
