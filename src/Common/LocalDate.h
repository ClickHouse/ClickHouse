#pragma once

#include <Common/DateLUT.h>
#include <base/DayNum.h>

#include <string>

/** Stores a calendar date in broken-down form (year, month, day-in-month).
  * Could be initialized from date in text form, like '2011-01-01' or from time_t with rounding to date.
  * Also could be initialized from date in text form like '20110101... (only first 8 symbols are used).
  * Could be implicitly cast to time_t.
  * NOTE: Transforming between time_t and LocalDate is done in local time zone!
  *
  * When local time was shifted backwards (due to daylight saving time or whatever reason)
  *  - then to resolve the ambiguity of transforming to time_t, lowest of two possible values is selected.
  *
  * packed - for memcmp to work naturally (but because m_year is 2 bytes, on little endian, comparison is correct only before year 2047)
  */
class LocalDate
{
private:
    unsigned short m_year; /// NOLINT
    unsigned char m_month;
    unsigned char m_day;

    void init(time_t time, const DateLUTImpl & date_lut);
    void init(const char * s, size_t length);

public:
    explicit LocalDate(time_t time, const DateLUTImpl & time_zone = DateLUT::instance())
    {
        init(time, time_zone);
    }

    explicit LocalDate(DayNum day_num, const DateLUTImpl & time_zone = DateLUT::instance());

    explicit LocalDate(ExtendedDayNum day_num, const DateLUTImpl & time_zone = DateLUT::instance());

    LocalDate(unsigned short year_, unsigned char month_, unsigned char day_) /// NOLINT
        : m_year(year_), m_month(month_), m_day(day_)
    {
    }

    explicit LocalDate(const std::string & s)
    {
        init(s.data(), s.size());
    }

    LocalDate(const char * data, size_t length)
    {
        init(data, length);
    }

    LocalDate() : m_year(0), m_month(0), m_day(0)
    {
    }

    LocalDate(const LocalDate &) noexcept = default;
    LocalDate & operator= (const LocalDate &) noexcept = default;

    DayNum getDayNum(const DateLUTImpl & lut = DateLUT::instance()) const;

    ExtendedDayNum getExtenedDayNum(const DateLUTImpl & lut = DateLUT::instance()) const;

    operator DayNum() const /// NOLINT
    {
        return getDayNum();
    }

    unsigned short year() const { return m_year; } /// NOLINT
    unsigned char month() const { return m_month; }
    unsigned char day() const { return m_day; }

    void year(unsigned short x) { m_year = x; } /// NOLINT
    void month(unsigned char x) { m_month = x; }
    void day(unsigned char x) { m_day = x; }

    bool operator< (const LocalDate & other) const
    {
        return 0 > memcmp(this, &other, sizeof(*this));
    }

    bool operator> (const LocalDate & other) const
    {
        return 0 < memcmp(this, &other, sizeof(*this));
    }

    bool operator<= (const LocalDate & other) const
    {
        return 0 >= memcmp(this, &other, sizeof(*this));
    }

    bool operator>= (const LocalDate & other) const
    {
        return 0 <= memcmp(this, &other, sizeof(*this));
    }

    bool operator== (const LocalDate & other) const
    {
        return 0 == memcmp(this, &other, sizeof(*this));
    }

    bool operator!= (const LocalDate & other) const
    {
        return !(*this == other);
    }
};

static_assert(sizeof(LocalDate) == 4);
