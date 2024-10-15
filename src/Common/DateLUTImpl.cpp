#include <Core/DecimalFunctions.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <memory>

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <cctz/zone_info_source.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

/// Embedded timezones.
std::string_view getTimeZone(const char * name);


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
    }
}

inline cctz::time_point<cctz::seconds> lookupTz(const cctz::time_zone & cctz_time_zone, const cctz::civil_day & date)
{
    cctz::time_zone::civil_lookup lookup = cctz_time_zone.lookup(date);

    /// Ambiguity is possible if time was changed backwards at the midnight
    /// or after midnight time has been changed back to midnight, for example one hour backwards at 01:00
    /// or after midnight time has been changed to the previous day, for example two hours backwards at 01:00
    /// Then midnight appears twice. Usually time change happens exactly at 00:00 or 01:00.

    /// If transition did not involve previous day, we should use the first midnight as the start of the day,
    /// otherwise it's better to use the second midnight.

    return lookup.trans < lookup.post
        ? lookup.post /* Second midnight appears after transition, so there was a piece of previous day after transition */
        : lookup.pre;
}

}

__attribute__((__weak__)) extern bool inside_main;

DateLUTImpl::DateLUTImpl(const std::string & time_zone_)
    : time_zone(time_zone_)
{
    /// DateLUT should not be initialized in global constructors for the following reasons:
    /// 1. It is too heavy.
    if (&inside_main)
        assert(inside_main);

    cctz::time_zone cctz_time_zone;
    if (!cctz::load_time_zone(time_zone, &cctz_time_zone))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot load time zone {}", time_zone_);

    constexpr cctz::civil_day epoch{1970, 1, 1};
    constexpr cctz::civil_day lut_start{DATE_LUT_MIN_YEAR, 1, 1};
    time_t start_of_day;

    /// Note: it's validated against all timezones in the system.
    static_assert((epoch - lut_start) == daynum_offset_epoch);

    offset_at_start_of_epoch = cctz_time_zone.lookup(cctz_time_zone.lookup(epoch).pre).offset;
    offset_at_start_of_lut = cctz_time_zone.lookup(cctz_time_zone.lookup(lut_start).pre).offset;
    offset_is_whole_number_of_hours_during_epoch = true;
    offset_is_whole_number_of_minutes_during_epoch = true;

    cctz::civil_day date = lut_start;
    cctz::time_point<cctz::seconds> start_of_day_time_point_if_no_transitions = lookupTz(cctz_time_zone, date);

    auto next_transition_date = date;

    /// Fill the lookup table:
    /// Adjustments only occur at the dates of transitions. We save next_transition_date and add 24h to the
    /// previous value until we reach the it. Then we do the adjustment and get the new next_transition_date.
    UInt32 i = 0;
    do
    {
        Values & values = lut[i];

        values.time_at_offset_change_value = 0;
        values.amount_of_offset_change_value = 0;

        if (date >= next_transition_date)
        {
            start_of_day_time_point_if_no_transitions = lookupTz(cctz_time_zone, date);

            /// If UTC offset was changed this day.
            /// Change in time zone without transition is possible, e.g. Moscow 1991 Sun, 31 Mar, 02:00 MSK to EEST
            cctz::time_zone::civil_transition transition{};
            if (cctz_time_zone.next_transition(start_of_day_time_point_if_no_transitions - std::chrono::seconds(1), &transition)
                && (cctz::civil_day(transition.from) == date || cctz::civil_day(transition.to) == date)
                && transition.from != transition.to)
            {
                values.time_at_offset_change_value = (transition.from - cctz::civil_second(date)) / Values::OffsetChangeFactor;
                values.amount_of_offset_change_value = (transition.to - transition.from) / Values::OffsetChangeFactor;

                /// We don't support too large changes.
                if (values.amount_of_offset_change_value > 24 * 4)
                    values.amount_of_offset_change_value = 24 * 4;
                else if (values.amount_of_offset_change_value < -24 * 4)
                    values.amount_of_offset_change_value = -24 * 4;

                /// We don't support cases when time change results in switching to previous day.
                /// Shift the point of time change later.
                if (values.time_at_offset_change_value + values.amount_of_offset_change_value < 0)
                    values.time_at_offset_change_value = -values.amount_of_offset_change_value;
            }

            next_transition_date = std::min(cctz::civil_day(transition.to), cctz::civil_day(transition.from));
        }

        start_of_day = std::chrono::system_clock::to_time_t(start_of_day_time_point_if_no_transitions);

        values.year = date.year();
        values.month = date.month();
        values.day_of_month = date.day();
        values.day_of_week = getDayOfWeek(date);
        values.date = start_of_day;

        assert(values.year >= DATE_LUT_MIN_YEAR && values.year <= DATE_LUT_MAX_YEAR + 1);
        assert(values.month >= 1 && values.month <= 12);
        assert(values.day_of_month >= 1 && values.day_of_month <= 31);
        assert(values.day_of_week >= 1 && values.day_of_week <= 7);

        if (values.day_of_month == 1)
        {
            cctz::civil_month month(date);
            values.days_in_month = cctz::civil_day(month + 1) - cctz::civil_day(month);
        }
        else
            values.days_in_month = i != 0 ? lut[i - 1].days_in_month : 31;

        if (offset_is_whole_number_of_hours_during_epoch && start_of_day > 0 && start_of_day % 3600)
            offset_is_whole_number_of_hours_during_epoch = false;

        if (offset_is_whole_number_of_minutes_during_epoch && start_of_day > 0 && start_of_day % 60)
            offset_is_whole_number_of_minutes_during_epoch = false;

        /// Going to next day.
        start_of_day_time_point_if_no_transitions += std::chrono::hours(24);
        ++date;
        ++i;
    }
    while (i < DATE_LUT_SIZE && lut[i - 1].year <= DATE_LUT_MAX_YEAR);

    /// Fill excessive part of lookup table. This is needed only to simplify handling of overflow cases.
    while (i < DATE_LUT_SIZE)
    {
        lut[i] = lut[i - 1];
        ++i;
    }

    /// Fill lookup table for years and months.
    size_t year_months_lut_index = 0;
    unsigned first_day_of_last_month = 0;

    for (unsigned day = 0; day < DATE_LUT_SIZE; ++day)
    {
        const Values & values = lut[day];

        if (values.day_of_month == 1)
        {
            if (values.month == 1)
                years_lut[values.year - DATE_LUT_MIN_YEAR] = day;

            year_months_lut_index = (values.year - DATE_LUT_MIN_YEAR) * 12 + values.month - 1;
            years_months_lut[year_months_lut_index] = day;
            first_day_of_last_month = day;
        }
    }

    /// Fill the rest of lookup table with the same last month (2106-02-01).
    for (; year_months_lut_index < DATE_LUT_YEARS * 12; ++year_months_lut_index)
    {
        years_months_lut[year_months_lut_index] = first_day_of_last_month;
    }

    /// Fill saturated LUT.
    {
        ssize_t day = DATE_LUT_SIZE - 1;
        for (; day >= 0; --day)
        {
            if (lut[day].date >= 0)
                lut_saturated[day] = lut[day];
            else
                break;
        }
        for (; day >= 0; --day)
            lut_saturated[day] = lut_saturated[day + 1];
    }
}

unsigned int DateLUTImpl::toMillisecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const
{
    constexpr Int64 millisecond_multiplier = 1'000;
    constexpr Int64 microsecond_multiplier = 1'000 * millisecond_multiplier;
    constexpr Int64 divider = microsecond_multiplier / millisecond_multiplier;

    auto components = DB::DecimalUtils::splitWithScaleMultiplier(datetime, scale_multiplier);

    if (datetime.value < 0 && components.fractional)
    {
        components.fractional = scale_multiplier + (components.whole ? Int64(-1) : Int64(1)) * components.fractional;
        --components.whole;
    }
    Int64 fractional = components.fractional;
    if (scale_multiplier > microsecond_multiplier)
        fractional = fractional / (scale_multiplier / microsecond_multiplier);
    else if (scale_multiplier < microsecond_multiplier)
        fractional = fractional * (microsecond_multiplier / scale_multiplier);

    UInt16 millisecond = static_cast<UInt16>(fractional / divider);
    return millisecond;
}


/// Prefer to load timezones from blobs linked to the binary.
/// The blobs are provided by "tzdata" library.
/// This allows to avoid dependency on system tzdata.
namespace cctz_extension
{
    namespace
    {
        class Source : public cctz::ZoneInfoSource
        {
        public:
            Source(const char * data_, size_t size_) : data(data_), size(size_) {}

            size_t Read(void * buf, size_t bytes) override
            {
                bytes = std::min(bytes, size);
                memcpy(buf, data, bytes);
                data += bytes;
                size -= bytes;
                return bytes;
            }

            int Skip(size_t offset) override
            {
                if (offset <= size)
                {
                    data += offset;
                    size -= offset;
                    return 0;
                }

                errno = EINVAL;
                return -1;
            }
        private:
            const char * data;
            size_t size;
        };

        std::unique_ptr<cctz::ZoneInfoSource> custom_factory(
            const std::string & name,
            const std::function<std::unique_ptr<cctz::ZoneInfoSource>(const std::string & name)> & fallback)
        {
            std::string_view tz_file = getTimeZone(name.data());

            if (!tz_file.empty())
                return std::make_unique<Source>(tz_file.data(), tz_file.size());

            return fallback(name);
        }
    }

    ZoneInfoSourceFactory zone_info_source_factory = custom_factory;
}
