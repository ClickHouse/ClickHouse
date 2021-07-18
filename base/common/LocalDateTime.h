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

    /// For struct to fill 8 bytes and for safe invocation of memcmp.
    /// NOTE We may use attribute packed instead, but it is less portable.
    unsigned char pad = 0;

    void init(time_t time, const DateLUTImpl & time_zone)
    {
        DateLUTImpl::DateTimeComponents components = time_zone.toDateTimeComponents(time);

        m_year = components.date.year;
        m_month = components.date.month;
        m_day = components.date.day;
        m_hour = components.time.hour;
        m_minute = components.time.minute;
        m_second = components.time.second;

        (void)pad;  /// Suppress unused private field warning.
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

        (void)pad;
    }

public:
    explicit LocalDateTime(time_t time, const DateLUTImpl & time_zone = DateLUT::instance())
    {
        init(time, time_zone);
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
    LocalDateTime toStartOfDate() const { return LocalDateTime(m_year, m_month, m_day, 0, 0, 0); }

    std::string toString() const
    {
        std::string s{"0000-00-00 00:00:00"};

        s[0] += m_year / 1000;
        s[1] += (m_year / 100) % 10;
        s[2] += (m_year / 10) % 10;
        s[3] += m_year % 10;
        s[5] += m_month / 10;
        s[6] += m_month % 10;
        s[8] += m_day / 10;
        s[9] += m_day % 10;

        s[11] += m_hour / 10;
        s[12] += m_hour % 10;
        s[14] += m_minute / 10;
        s[15] += m_minute % 10;
        s[17] += m_second / 10;
        s[18] += m_second % 10;

        return s;
    }

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

static_assert(sizeof(LocalDateTime) == 8);
