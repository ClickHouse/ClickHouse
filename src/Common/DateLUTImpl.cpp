#include <Core/DecimalFunctions.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <cctz/zone_info_source.h>
#pragma clang diagnostic pop


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

/// Embedded timezones.
std::string_view getTimeZone(const char * name);  /// NOLINT(misc-use-internal-linkage)


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

DateLUTImpl::DateLUTImpl(std::string_view time_zone_) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - lut and lut_saturated are fully assigned below
    : time_zone(time_zone_)
{
    /// DateLUT should not be initialized in global constructors for the following reasons:
    /// 1. It is too heavy.
    if (&inside_main)
        chassert(inside_main);

    /// The loaded time zone is kept as a member, so the out-of-range escape paths can use cctz directly.
    cctz_time_zone = std::make_unique<cctz::time_zone>();
    if (!cctz::load_time_zone(time_zone, cctz_time_zone.get()))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Cannot load time zone {}", time_zone_);

    const cctz::time_zone & tz = *cctz_time_zone;

    constexpr cctz::civil_day epoch{1970, 1, 1};
    constexpr cctz::civil_day lut_start{DATE_LUT_MIN_YEAR, 1, 1};
    time_t start_of_day = 0;

    /// Note: it's validated against all timezones in the system.
    static_assert((epoch - lut_start) == daynum_offset_epoch);

    offset_at_start_of_epoch = tz.lookup(tz.lookup(epoch).pre).offset;
    offset_at_start_of_lut = tz.lookup(tz.lookup(lut_start).pre).offset;
    offset_is_whole_number_of_hours_during_epoch = true;
    offset_is_whole_number_of_minutes_during_epoch = true;
    offset_is_fixed = true;

    cctz::civil_day date = lut_start;
    cctz::time_point<cctz::seconds> start_of_day_time_point_if_no_transitions = lookupTz(tz, date);

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
            start_of_day_time_point_if_no_transitions = lookupTz(tz, date);

            /// If UTC offset was changed this day.
            /// Change in time zone without transition is possible, e.g. Moscow 1991 Sun, 31 Mar, 02:00 MSK to EEST
            cctz::time_zone::civil_transition transition{};
            if (tz.next_transition(start_of_day_time_point_if_no_transitions - std::chrono::seconds(1), &transition)
                && (cctz::civil_day(transition.from) == date || cctz::civil_day(transition.to) == date)
                && transition.from != transition.to)
            {
                values.time_at_offset_change_value = static_cast<UInt8>((transition.from - cctz::civil_second(date)) / Values::OffsetChangeFactor);
                values.amount_of_offset_change_value = static_cast<Int8>((transition.to - transition.from) / Values::OffsetChangeFactor);

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

        values.year = static_cast<UInt16>(date.year());
        values.month = static_cast<UInt8>(date.month());
        values.day_of_month = static_cast<UInt8>(date.day());
        values.day_of_week = getDayOfWeek(date);
        values.date = start_of_day;

        chassert(values.year >= DATE_LUT_MIN_YEAR && values.year <= DATE_LUT_MAX_YEAR + 1);
        chassert(values.month >= 1 && values.month <= 12);
        chassert(values.day_of_month >= 1 && values.day_of_month <= 31);
        chassert(values.day_of_week >= 1 && values.day_of_week <= 7);

        if (values.day_of_month == 1)
        {
            cctz::civil_month month(date);
            values.days_in_month = static_cast<UInt8>(cctz::civil_day(month + 1) - cctz::civil_day(month));
        }
        else
            values.days_in_month = i != 0 ? lut[i - 1].days_in_month : 31;

        if (offset_is_whole_number_of_hours_during_epoch && start_of_day > 0 && start_of_day % 3600)
            offset_is_whole_number_of_hours_during_epoch = false;

        if (offset_is_whole_number_of_minutes_during_epoch && start_of_day > 0 && start_of_day % 60)
            offset_is_whole_number_of_minutes_during_epoch = false;

        /// An offset change at midnight makes consecutive days start more or less than 86400 seconds apart;
        /// a change at any other time is recorded in amount_of_offset_change_value of the affected day.
        if (values.amount_of_offset_change_value != 0 || (i != 0 && values.date - lut[i - 1].date != 86400))
            offset_is_fixed = false;

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

DateLUTImpl::~DateLUTImpl() = default;

namespace
{

/// Number of days between DATE_LUT_MIN_YEAR-01-01 and the given civil day. Mirrors the LUTIndex value space.
Int64 dayIndexOfCivilDay(cctz::civil_day day)
{
    return day - cctz::civil_day{DATE_LUT_MIN_YEAR, 1, 1};
}

/// Number of days in the given calendar month.
int daysInCivilMonth(Int64 year, int month)
{
    const cctz::civil_day first{static_cast<int>(year), month, 1};
    return static_cast<int>(cctz::civil_day{cctz::civil_month{first} + 1} - first);
}

}

Int64 DateLUTImpl::findDayIndexOutOfRange(Time t) const
{
    /// Bound the timestamp before constructing the time point to avoid chrono overflow on extreme inputs
    /// (e.g. fromUnixTimestamp64 with a huge argument); the local day is clamped to [0000, 9999] afterwards.
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    const cctz::civil_second cs = cctz::convert(std::chrono::system_clock::from_time_t(t), *cctz_time_zone);
    return std::clamp(dayIndexOfCivilDay(cctz::civil_day{cs}), min_representable_day_index, max_representable_day_index);
}

DateLUTImpl::Values DateLUTImpl::valuesForOutOfRangeDayIndex(Int64 day_index) const
{
    /// The fast path returns a copy of the entry from the table; for out-of-range indices everything is computed with cctz.
    if (day_index >= 0 && day_index < DATE_LUT_SIZE) [[likely]]
        return lut[day_index];

    /// Clamp to the representable window so that the 4-digit year never overflows during formatting.
    day_index = std::clamp(day_index, min_representable_day_index, max_representable_day_index);
    const cctz::civil_day date = cctz::civil_day{DATE_LUT_MIN_YEAR, 1, 1} + day_index;

    Values values{};
    values.year = static_cast<UInt16>(date.year());
    values.month = static_cast<UInt8>(date.month());
    values.day_of_month = static_cast<UInt8>(date.day());
    values.day_of_week = getDayOfWeek(date);
    values.date = std::chrono::system_clock::to_time_t(lookupTz(*cctz_time_zone, date));

    const cctz::civil_month month(date);
    values.days_in_month = static_cast<UInt8>(cctz::civil_day(month + 1) - cctz::civil_day(month));

    /// The offset-change fields (used for intra-day time decomposition) are intentionally left zero here:
    /// methods that need them (toHour, toDateTimeComponents, ...) use cctz directly via toDateTimeComponentsOutOfRange.
    return values;
}

DateLUTImpl::DateTimeComponents DateLUTImpl::toDateTimeComponentsOutOfRange(Time t) const
{
    /// Only bound enough to avoid chrono overflow; the local civil result is clamped to a valid year below.
    /// Clamping the UTC timestamp to the representable window here would shift valid local boundary values
    /// (e.g. for a positive-offset zone, local 0000-01-01 00:00:00 maps to a UTC instant before the window).
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    cctz::civil_second cs = cctz::convert(std::chrono::system_clock::from_time_t(t), *cctz_time_zone);

    /// A non-zero UTC offset can push the local year just past the representable window; clamp it to a valid 4-digit year.
    if (cs.year() > DATE_LUT_MAX_REPRESENTABLE_YEAR)
        cs = cctz::civil_second{DATE_LUT_MAX_REPRESENTABLE_YEAR, 12, 31, 23, 59, 59};
    else if (cs.year() < DATE_LUT_MIN_REPRESENTABLE_YEAR)
        cs = cctz::civil_second{DATE_LUT_MIN_REPRESENTABLE_YEAR, 1, 1, 0, 0, 0};

    DateTimeComponents res;
    res.date.year = static_cast<uint16_t>(cs.year());
    res.date.month = static_cast<uint8_t>(cs.month());
    res.date.day = static_cast<uint8_t>(cs.day());
    res.time.hour = static_cast<uint64_t>(cs.hour());
    res.time.minute = static_cast<uint8_t>(cs.minute());
    res.time.second = static_cast<uint8_t>(cs.second());
    return res;
}

DateLUTImpl::Time DateLUTImpl::toTimeOutOfRange(Time t) const
{
    /// Local time of day in seconds (counted from local midnight), shifted to be relative to the start of the epoch,
    /// exactly as the in-range toTime does (which returns the time of day starting at 1970-01-01 00:00:00 local time).
    /// Bound only enough to avoid chrono overflow, so local boundary values keep their correct time of day.
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    const cctz::time_zone & tz = *cctz_time_zone;
    const auto tp = std::chrono::system_clock::from_time_t(t);
    const cctz::civil_second cs = cctz::convert(tp, tz);
    const cctz::civil_day cd{cs};
    const Time start_of_day = std::chrono::system_clock::to_time_t(lookupTz(tz, cd));
    return (t - start_of_day) - offset_at_start_of_epoch;
}

DateLUTImpl::Time DateLUTImpl::timezoneOffsetOutOfRange(Time t) const
{
    /// The UTC offset of the time zone at the given time point.
    t = std::clamp(t, min_representable_time, max_representable_time);
    return cctz_time_zone->lookup(std::chrono::system_clock::from_time_t(t)).offset;
}

DateLUTImpl::Time DateLUTImpl::addDaysOutOfRange(Time t, Int64 delta) const
{
    /// Keep the same wall-clock time of day, but on the day that is `delta` days away.
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    const cctz::time_zone & tz = *cctz_time_zone;
    const cctz::civil_second cs = cctz::convert(std::chrono::system_clock::from_time_t(t), tz);
    /// Saturate to the representable calendar: a huge delta would otherwise move the day far outside
    /// [0000, 9999] and overflow the conversion back to a time point.
    const Int64 capped_delta = std::clamp(delta, min_representable_day_index - max_representable_day_index, max_representable_day_index - min_representable_day_index);
    const Int64 day_index = std::clamp(dayIndexOfCivilDay(cctz::civil_day{cs} + capped_delta), min_representable_day_index, max_representable_day_index);
    const cctz::civil_day target_day = cctz::civil_day{DATE_LUT_MIN_YEAR, 1, 1} + day_index;
    const cctz::civil_second target{target_day.year(), target_day.month(), target_day.day(), cs.hour(), cs.minute(), cs.second()};
    return std::chrono::system_clock::to_time_t(tz.lookup(target).pre);
}

DateLUTImpl::Time DateLUTImpl::addMonthsOutOfRange(Time t, Int64 delta) const
{
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    const cctz::time_zone & tz = *cctz_time_zone;
    const cctz::civil_second cs = cctz::convert(std::chrono::system_clock::from_time_t(t), tz);

    /// Cap the delta so the month index cannot overflow Int64 for a huge interval.
    const Int64 capped_delta = std::clamp<Int64>(delta, -12 * (DATE_LUT_MAX_REPRESENTABLE_YEAR + 1), 12 * (DATE_LUT_MAX_REPRESENTABLE_YEAR + 1));
    const Int64 month_index = static_cast<Int64>(cs.year()) * 12 + (cs.month() - 1) + capped_delta;
    const Int64 unclamped_year = month_index >= 0 ? month_index / 12 : -((-month_index + 11) / 12);

    /// If the result leaves the representable calendar, saturate the whole date to the boundary. Clamping only the
    /// year would keep a wrapped month (e.g. 9999-12-31 + 1 MONTH normalizes to year 10000, month 1, and clamping
    /// the year back to 9999 would jump to 9999-01-31, i.e. backwards in time).
    if (unclamped_year > DATE_LUT_MAX_REPRESENTABLE_YEAR)
        return std::chrono::system_clock::to_time_t(tz.lookup(cctz::civil_second{DATE_LUT_MAX_REPRESENTABLE_YEAR, 12, 31, 23, 59, 59}).pre);
    if (unclamped_year < DATE_LUT_MIN_REPRESENTABLE_YEAR)
        return std::chrono::system_clock::to_time_t(tz.lookup(cctz::civil_second{DATE_LUT_MIN_REPRESENTABLE_YEAR, 1, 1, 0, 0, 0}).pre);

    const int month = static_cast<int>(month_index - unclamped_year * 12) + 1;
    /// Saturate the day of month, e.g. 31 Jan + 1 month = 28/29 Feb.
    const int day_of_month = std::min<int>(cs.day(), daysInCivilMonth(unclamped_year, month));

    const cctz::civil_second target{static_cast<int>(unclamped_year), month, day_of_month, cs.hour(), cs.minute(), cs.second()};
    return std::chrono::system_clock::to_time_t(tz.lookup(target).pre);
}

DateLUTImpl::Time DateLUTImpl::addYearsOutOfRange(Time t, Int64 delta) const
{
    t = std::clamp(t, min_chrono_safe_time, max_chrono_safe_time);
    const cctz::time_zone & tz = *cctz_time_zone;
    const cctz::civil_second cs = cctz::convert(std::chrono::system_clock::from_time_t(t), tz);

    /// Saturate to the representable calendar so a huge delta cannot overflow the year computation or the
    /// conversion of the result back to a time point.
    const Int64 capped_delta = std::clamp<Int64>(delta, -(DATE_LUT_MAX_REPRESENTABLE_YEAR + 1), DATE_LUT_MAX_REPRESENTABLE_YEAR + 1);
    const Int64 year = std::clamp<Int64>(static_cast<Int64>(cs.year()) + capped_delta, DATE_LUT_MIN_REPRESENTABLE_YEAR, DATE_LUT_MAX_REPRESENTABLE_YEAR);
    int day_of_month = cs.day();
    /// Saturation to 28 Feb can happen for 29 Feb of a leap year mapped onto a non-leap year.
    if (cs.month() == 2 && day_of_month == 29)
        day_of_month = std::min(day_of_month, daysInCivilMonth(year, 2));

    const cctz::civil_second target{static_cast<int>(year), cs.month(), day_of_month, cs.hour(), cs.minute(), cs.second()};
    return std::chrono::system_clock::to_time_t(tz.lookup(target).pre);
}

ExtendedDayNum DateLUTImpl::addMonthsOutOfRange(ExtendedDayNum d, Int64 delta) const
{
    const cctz::civil_day cd = cctz::civil_day{DATE_LUT_MIN_YEAR, 1, 1} + outOfRangeDayIndex(d);

    /// Cap the delta so the month index cannot overflow Int64 for a huge interval; the day index is clamped
    /// to the representable calendar below.
    const Int64 capped_delta = std::clamp<Int64>(delta, -12 * (DATE_LUT_MAX_REPRESENTABLE_YEAR + 1), 12 * (DATE_LUT_MAX_REPRESENTABLE_YEAR + 1));
    const Int64 month_index = static_cast<Int64>(cd.year()) * 12 + (cd.month() - 1) + capped_delta;
    const Int64 unclamped_year = month_index >= 0 ? month_index / 12 : -((-month_index + 11) / 12);

    /// Saturate the whole date to the boundary rather than clamping only the year (which would keep a wrapped month).
    if (unclamped_year > DATE_LUT_MAX_REPRESENTABLE_YEAR)
        return dayNumOfDayIndex(dayIndexOfCivilDay(cctz::civil_day{DATE_LUT_MAX_REPRESENTABLE_YEAR, 12, 31}));
    if (unclamped_year < DATE_LUT_MIN_REPRESENTABLE_YEAR)
        return dayNumOfDayIndex(dayIndexOfCivilDay(cctz::civil_day{DATE_LUT_MIN_REPRESENTABLE_YEAR, 1, 1}));

    const int month = static_cast<int>(month_index - unclamped_year * 12) + 1;
    const int day_of_month = std::min<int>(cd.day(), daysInCivilMonth(unclamped_year, month));

    return dayNumOfDayIndex(dayIndexOfCivilDay(cctz::civil_day{static_cast<int>(unclamped_year), month, day_of_month}));
}

ExtendedDayNum DateLUTImpl::addYearsOutOfRange(ExtendedDayNum d, Int64 delta) const
{
    const cctz::civil_day cd = cctz::civil_day{DATE_LUT_MIN_YEAR, 1, 1} + outOfRangeDayIndex(d);

    /// Saturate to the representable calendar so a huge delta cannot overflow the year computation.
    const Int64 capped_delta = std::clamp<Int64>(delta, -(DATE_LUT_MAX_REPRESENTABLE_YEAR + 1), DATE_LUT_MAX_REPRESENTABLE_YEAR + 1);
    const Int64 year = std::clamp<Int64>(static_cast<Int64>(cd.year()) + capped_delta, DATE_LUT_MIN_REPRESENTABLE_YEAR, DATE_LUT_MAX_REPRESENTABLE_YEAR);
    int day_of_month = cd.day();
    if (cd.month() == 2 && day_of_month == 29)
        day_of_month = std::min(day_of_month, daysInCivilMonth(year, 2));

    return dayNumOfDayIndex(dayIndexOfCivilDay(cctz::civil_day{static_cast<int>(year), cd.month(), day_of_month}));
}

DateLUTImpl::Time DateLUTImpl::makeDateOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month) const
{
    /// Saturate the year to the representable calendar so converting the result to a time point cannot overflow.
    year = std::clamp<Int64>(year, DATE_LUT_MIN_REPRESENTABLE_YEAR, DATE_LUT_MAX_REPRESENTABLE_YEAR);
    const cctz::civil_day date{static_cast<int>(year), month, day_of_month};
    return std::chrono::system_clock::to_time_t(lookupTz(*cctz_time_zone, date));
}

DateLUTImpl::Time DateLUTImpl::makeDateTimeOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const
{
    /// Saturate the year to the representable calendar so converting the result to a time point cannot overflow.
    year = std::clamp<Int64>(year, DATE_LUT_MIN_REPRESENTABLE_YEAR, DATE_LUT_MAX_REPRESENTABLE_YEAR);
    /// In case of ambiguity (clock moved backwards) we choose the greater timestamp, matching the in-range makeDateTime.
    const cctz::civil_second cs{static_cast<int>(year), month, day_of_month, hour, minute, second};
    return std::chrono::system_clock::to_time_t(cctz_time_zone->lookup(cs).post);
}

ExtendedDayNum DateLUTImpl::makeDayNumOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month) const
{
    year = std::clamp<Int64>(year, DATE_LUT_MIN_REPRESENTABLE_YEAR, DATE_LUT_MAX_REPRESENTABLE_YEAR);
    const cctz::civil_day date{static_cast<int>(year), month, day_of_month};
    return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(dayIndexOfCivilDay(date) - daynum_offset_epoch)};
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


unsigned int DateLUTImpl::toMicrosecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const
{
    constexpr Int64 microsecond_multiplier = 1'000'000;

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

    return static_cast<unsigned>(fractional);
}


unsigned int DateLUTImpl::toNanosecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const
{
    constexpr Int64 nanosecond_multiplier = 1'000'000'000;

    auto components = DB::DecimalUtils::splitWithScaleMultiplier(datetime, scale_multiplier);

    if (datetime.value < 0 && components.fractional)
    {
        components.fractional = scale_multiplier + (components.whole ? Int64(-1) : Int64(1)) * components.fractional;
        --components.whole;
    }
    Int64 fractional = components.fractional;
    if (scale_multiplier > nanosecond_multiplier)
        fractional = fractional / (scale_multiplier / nanosecond_multiplier);
    else if (scale_multiplier < nanosecond_multiplier)
        fractional = fractional * (nanosecond_multiplier / scale_multiplier);

    return static_cast<unsigned>(fractional);
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
