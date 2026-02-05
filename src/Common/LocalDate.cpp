#include <Common/DateLUTImpl.h>
#include <Common/LocalDate.h>

#include <stdexcept>

void LocalDate::init(time_t time, const DateLUTImpl & date_lut)
{
    const auto & values = date_lut.getValues(time);

    m_year = values.year;
    m_month = values.month;
    m_day = values.day_of_month;
}


void LocalDate::init(const char * s, size_t length)
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

LocalDate::LocalDate(DayNum day_num, const DateLUTImpl & time_zone)
{
    const auto & values = time_zone.getValues(day_num);
    m_year  = values.year;
    m_month = values.month;
    m_day   = values.day_of_month;
}

LocalDate::LocalDate(ExtendedDayNum day_num, const DateLUTImpl & time_zone)
{
    const auto & values = time_zone.getValues(day_num);
    m_year  = values.year;
    m_month = values.month;
    m_day   = values.day_of_month;
}

DayNum LocalDate::getDayNum(const DateLUTImpl & lut) const
{
    return DayNum(lut.makeDayNum(m_year, m_month, m_day).toUnderType());
}

ExtendedDayNum LocalDate::getExtenedDayNum(const DateLUTImpl & lut) const
{
    return ExtendedDayNum (lut.makeDayNum(m_year, m_month, m_day).toUnderType());
}
