#pragma once

#include <string.h>
#include <string>
#include <sstream>
#include <exception>
#include <common/DateLUT.h>


/** Stores a calendar date in broken-down form (year, month, day-in-month).
  * Could be initialized from date in text form, like '2011-01-01' or from time_t with rounding to date.
  * Also could be initialized from date in text form like '20110101... (only first 8 symbols are used).
  * Could be implicitly casted to time_t.
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
    unsigned short m_year;
    unsigned char m_month;
    unsigned char m_day;

    void init(time_t time)
    {
        const auto & date_lut = DateLUT::instance();
        const auto & values = date_lut.getValues(time);

        m_year = values.year;
        m_month = values.month;
        m_day = values.day_of_month;
    }

    void init(const char * s, size_t length)
    {
        if (length < 8)
            throw std::runtime_error("Cannot parse LocalDate: " + std::string(s, length));

        m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');

        if (s[4] == '-')
        {
            if (length < 10)
                throw std::runtime_error("Cannot parse LocalDate: " + std::string(s, length));
            m_month = (s[5] - '0') * 10 + (s[6] - '0');
            m_day = (s[8] - '0') * 10 + (s[9] - '0');
        }
        else
        {
            m_month = (s[4] -'0') * 10 + (s[5] -'0');
            m_day = (s[6] - '0')* 10 + (s[7] -'0');
        }
    }

public:
    explicit LocalDate(time_t time)
    {
        init(time);
    }

    LocalDate(DayNum day_num)
    {
        const auto & values = DateLUT::instance().getValues(day_num);
        m_year  = values.year;
        m_month = values.month;
        m_day   = values.day_of_month;
    }

    LocalDate(unsigned short year_, unsigned char month_, unsigned char day_)
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

    LocalDate & operator= (time_t time)
    {
        init(time);
        return *this;
    }

    operator time_t() const
    {
        return DateLUT::instance().makeDate(m_year, m_month, m_day);
    }

    DayNum getDayNum() const
    {
        return DateLUT::instance().makeDayNum(m_year, m_month, m_day);
    }

    operator DayNum() const
    {
        return getDayNum();
    }

    unsigned short year() const { return m_year; }
    unsigned char month() const { return m_month; }
    unsigned char day() const { return m_day; }

    void year(unsigned short x) { m_year = x; }
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

    /// NOTE Inefficient.
    std::string toString(char separator = '-') const
    {
        std::stringstream ss;
        if (separator)
            ss << year() << separator << (month() / 10) << (month() % 10)
                << separator << (day() / 10) << (day() % 10);
        else
            ss << year() << (month() / 10) << (month() % 10)
                << (day() / 10) << (day() % 10);
        return ss.str();
    }
};

inline std::ostream & operator<< (std::ostream & ostr, const LocalDate & date)
{
    return ostr << date.year()
        << '-' << (date.month() / 10) << (date.month() % 10)
        << '-' << (date.day() / 10) << (date.day() % 10);
}


namespace std
{
inline string to_string(const LocalDate & date)
{
    return date.toString();
}
}
