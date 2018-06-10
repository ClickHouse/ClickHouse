#pragma once

#include <string>
#include <iomanip>
#include <exception>
#include <common/DateLUT.h>
#include <common/LocalDate.h>


/** Stores calendar date and time in broken-down form.
  * Could be initialized from date and time in text form like '2011-01-01 00:00:00' or from time_t.
  * Could be implicitly casted to time_t.
  * NOTE: Transforming between time_t and LocalDate is done in local time zone!
  *
  * When local time was shifted backwards (due to daylight saving time or whatever reason)
  *  - then to resolve the ambiguity of transforming to time_t, lowest of two possible values is selected.
  *
  * packed - for memcmp to work naturally (but because m_year is 2 bytes, on little endian, comparison is correct only before year 2047)
  */
class LocalDateTime
{
private:
    unsigned short m_year;
    unsigned char m_month;
    unsigned char m_day;
    unsigned char m_hour;
    unsigned char m_minute;
    unsigned char m_second;

    void init(time_t time)
    {
        if (unlikely(time > DATE_LUT_MAX || time == 0))
        {
            m_year = 0;
            m_month = 0;
            m_day = 0;
            m_hour = 0;
            m_minute = 0;
            m_second = 0;

            return;
        }

        const auto & date_lut = DateLUT::instance();
        const auto & values = date_lut.getValues(time);

        m_year = values.year;
        m_month = values.month;
        m_day = values.day_of_month;
        m_hour = date_lut.toHour(time);
        m_minute = date_lut.toMinute(time);
        m_second = date_lut.toSecond(time);
    }

    void init(const char * s, size_t length)
    {
        if (length < 19)
            throw std::runtime_error("Cannot parse LocalDateTime: " + std::string(s, length));

        m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        m_month = (s[5] - '0') * 10 + (s[6] - '0');
        m_day = (s[8] - '0') * 10 + (s[9] - '0');

        m_hour = (s[11] - '0') * 10 + (s[12] - '0');
        m_minute = (s[14] - '0') * 10 + (s[15] - '0');
        m_second = (s[17] - '0') * 10 + (s[18] - '0');
    }

public:
    explicit LocalDateTime(time_t time)
    {
        init(time);
    }

    LocalDateTime(unsigned short year_, unsigned char month_, unsigned char day_,
        unsigned char hour_, unsigned char minute_, unsigned char second_)
        : m_year(year_), m_month(month_), m_day(day_), m_hour(hour_), m_minute(minute_), m_second(second_)
    {
    }

    explicit LocalDateTime(const std::string & s)
    {
        if (s.size() < 19)
            throw std::runtime_error("Cannot parse LocalDateTime: " + s);

        init(s.data(), s.size());
    }

    LocalDateTime() : m_year(0), m_month(0), m_day(0), m_hour(0), m_minute(0), m_second(0)
    {
    }

    LocalDateTime(const char * data, size_t length)
    {
        init(data, length);
    }

    LocalDateTime(const LocalDateTime &) noexcept = default;
    LocalDateTime & operator= (const LocalDateTime &) noexcept = default;

    LocalDateTime & operator= (time_t time)
    {
        init(time);
        return *this;
    }

    operator time_t() const
    {
        return m_year == 0
            ? 0
            : DateLUT::instance().makeDateTime(m_year, m_month, m_day, m_hour, m_minute, m_second);
    }

    unsigned short year() const { return m_year; }
    unsigned char month() const { return m_month; }
    unsigned char day() const { return m_day; }
    unsigned char hour() const { return m_hour; }
    unsigned char minute() const { return m_minute; }
    unsigned char second() const { return m_second; }

    void year(unsigned short x) { m_year = x; }
    void month(unsigned char x) { m_month = x; }
    void day(unsigned char x) { m_day = x; }
    void hour(unsigned char x) { m_hour = x; }
    void minute(unsigned char x) { m_minute = x; }
    void second(unsigned char x) { m_second = x; }

    LocalDate toDate() const { return LocalDate(m_year, m_month, m_day); }

    LocalDateTime toStartOfDate() { return LocalDateTime(m_year, m_month, m_day, 0, 0, 0); }

    bool operator< (const LocalDateTime & other) const
    {
        return 0 > memcmp(this, &other, sizeof(*this));
    }

    bool operator> (const LocalDateTime & other) const
    {
        return 0 < memcmp(this, &other, sizeof(*this));
    }

    bool operator<= (const LocalDateTime & other) const
    {
        return 0 >= memcmp(this, &other, sizeof(*this));
    }

    bool operator>= (const LocalDateTime & other) const
    {
        return 0 <= memcmp(this, &other, sizeof(*this));
    }

    bool operator== (const LocalDateTime & other) const
    {
        return 0 == memcmp(this, &other, sizeof(*this));
    }

    bool operator!= (const LocalDateTime & other) const
    {
        return !(*this == other);
    }
};

inline std::ostream & operator<< (std::ostream & ostr, const LocalDateTime & datetime)
{
    ostr << std::setfill('0') << std::setw(4) << datetime.year();

    ostr << '-' << (datetime.month() / 10) << (datetime.month() % 10)
        << '-' << (datetime.day() / 10) << (datetime.day() % 10)
        << ' ' << (datetime.hour() / 10) << (datetime.hour() % 10)
        << ':' << (datetime.minute() / 10) << (datetime.minute() % 10)
        << ':' << (datetime.second() / 10) << (datetime.second() % 10);

    return ostr;
}


namespace std
{
inline string to_string(const LocalDateTime & datetime)
{
    stringstream str;
    str << datetime;
    return str.str();
}
}
