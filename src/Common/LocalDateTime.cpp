#include <Common/DateLUTImpl.h>
#include <Common/LocalDateTime.h>

void LocalDateTime::init(time_t time, const DateLUTImpl & time_zone)
{
    DateLUTImpl::DateTimeComponents components = time_zone.toDateTimeComponents(static_cast<DateLUTImpl::Time>(time));

    m_year = components.date.year;
    m_month = components.date.month;
    m_day = components.date.day;
    m_hour = components.time.hour;
    m_minute = components.time.minute;
    m_second = components.time.second;

    (void)pad;  /// Suppress unused private field warning.
}

time_t LocalDateTime::to_time_t(const DateLUTImpl & time_zone) const
{
    return time_zone.makeDateTime(m_year, m_month, m_day, m_hour, m_minute, m_second);
}
