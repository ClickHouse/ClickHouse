#if __has_include(<cctz/civil_time.h>)
#include <cctz/civil_time.h> // bundled, debian
#else
#include <civil_time.h> // freebsd
#endif

#if __has_include(<cctz/time_zone.h>)
#include <cctz/time_zone.h>
#else
#include <time_zone.h>
#endif

#include <common/DateLUTImpl.h>
#include <Poco/Exception.h>

#include <memory>
#include <chrono>
#include <cstring>
#include <iostream>

#define DATE_LUT_MIN 0


namespace
{

UInt8 getDayOfWeek(const cctz::civil_day & date)
{
    cctz::weekday day_of_week = cctz::get_weekday(date);
    switch (day_of_week)
    {
        case cctz::weekday::monday:     return 1;
        case cctz::weekday::tuesday:    return 2;
        case cctz::weekday::wednesday:  return 3;
        case cctz::weekday::thursday:   return 4;
        case cctz::weekday::friday:     return 5;
        case cctz::weekday::saturday:   return 6;
        case cctz::weekday::sunday:     return 7;
        default:
            throw Poco::Exception("Logical error: incorrect week day.");
    }
}

}


DateLUTImpl::DateLUTImpl(const std::string & time_zone_)
    : time_zone(time_zone_)
{
    size_t i = 0;
    time_t start_of_day = DATE_LUT_MIN;

    cctz::time_zone cctz_time_zone;
    if (!cctz::load_time_zone(time_zone.data(), &cctz_time_zone))
        throw Poco::Exception("Cannot load time zone " + time_zone_);

    cctz::time_zone::absolute_lookup start_of_epoch_lookup = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(start_of_day));
    offset_at_start_of_epoch = start_of_epoch_lookup.offset;
    offset_is_whole_number_of_hours_everytime = true;

    cctz::civil_day date{1970, 1, 1};

    do
    {
        cctz::time_zone::civil_lookup lookup = cctz_time_zone.lookup(date);

        start_of_day = std::chrono::system_clock::to_time_t(lookup.pre);    /// Ambiguity is possible.

        Values & values = lut[i];
        values.year = date.year();
        values.month = date.month();
        values.day_of_month = date.day();
        values.day_of_week = getDayOfWeek(date);
        values.date = start_of_day;

        if (values.day_of_month == 1)
        {
            cctz::civil_month month(date);
            values.days_in_month = cctz::civil_day(month + 1) - cctz::civil_day(month);
        }
        else
            values.days_in_month = i != 0 ? lut[i - 1].days_in_month : 31;

        values.time_at_offset_change = 0;
        values.amount_of_offset_change = 0;

        if (start_of_day % 3600)
            offset_is_whole_number_of_hours_everytime = false;

        /// If UTC offset was changed in previous day.
        if (i != 0)
        {
            auto amount_of_offset_change_at_prev_day = 86400 - (lut[i].date - lut[i - 1].date);
            if (amount_of_offset_change_at_prev_day)
            {
                lut[i - 1].amount_of_offset_change = amount_of_offset_change_at_prev_day;

                const auto utc_offset_at_beginning_of_day = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(lut[i - 1].date)).offset;

                /// Find a time (timestamp offset from beginning of day),
                ///  when UTC offset was changed. Search is performed with 15-minute granularity, assuming it is enough.

                time_t time_at_offset_change = 900;
                while (time_at_offset_change < 65536)
                {
                    auto utc_offset_at_current_time = cctz_time_zone.lookup(std::chrono::system_clock::from_time_t(
                        lut[i - 1].date + time_at_offset_change)).offset;

                    if (utc_offset_at_current_time != utc_offset_at_beginning_of_day)
                        break;

                    time_at_offset_change += 900;
                }

                lut[i - 1].time_at_offset_change = time_at_offset_change >= 65536 ? 0 : time_at_offset_change;

/*                std::cerr << lut[i - 1].year << "-" << int(lut[i - 1].month) << "-" << int(lut[i - 1].day_of_month)
                    << " offset was changed at " << lut[i - 1].time_at_offset_change << " for " << lut[i - 1].amount_of_offset_change << " seconds.\n";*/
            }
        }

        /// Going to next day.
        ++date;
        ++i;
    }
    while (start_of_day <= DATE_LUT_MAX && i <= DATE_LUT_MAX_DAY_NUM);

    /// Fill excessive part of lookup table. This is needed only to simplify handling of overflow cases.
    while (i < DATE_LUT_SIZE)
    {
        lut[i] = lut[0];
        ++i;
    }

    /// Fill lookup table for years and months.
    for (size_t day = 0; day < DATE_LUT_SIZE && lut[day].year <= DATE_LUT_MAX_YEAR; ++day)
    {
        const Values & values = lut[day];

        if (values.day_of_month == 1)
        {
            if (values.month == 1)
                years_lut[values.year - DATE_LUT_MIN_YEAR] = day;
            years_months_lut[(values.year - DATE_LUT_MIN_YEAR) * 12 + values.month - 1] = day;
        }
    }
}
