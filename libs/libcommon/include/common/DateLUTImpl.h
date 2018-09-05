#pragma once

#include <common/Types.h>
#include <common/likely.h>
#include <common/strong_typedef.h>
#include <ctime>
#include <string>

#define DATE_LUT_MAX (0xFFFFFFFFU - 86400)
#define DATE_LUT_MAX_DAY_NUM (0xFFFFFFFFU / 86400)
/// Table size is bigger than DATE_LUT_MAX_DAY_NUM to fill all indices within UInt16 range: this allows to remove extra check.
#define DATE_LUT_SIZE 0x10000
#define DATE_LUT_MIN_YEAR 1970
#define DATE_LUT_MAX_YEAR 2105 /// Last supported year
#define DATE_LUT_YEARS (1 + DATE_LUT_MAX_YEAR - DATE_LUT_MIN_YEAR) /// Number of years in lookup table


STRONG_TYPEDEF(UInt16, DayNum)


/** Lookup table to conversion of time to date, and to month / year / day of week / day of month and so on.
  * First time was implemented for OLAPServer, that needed to do billions of such transformations.
  */
class DateLUTImpl
{
public:
    DateLUTImpl(const std::string & time_zone);

public:
    struct Values
    {
        /// Least significat 32 bits from time_t at beginning of the day.
        /// If the unix timestamp of beginning of the day is negative (example: 1970-01-01 MSK, where time_t == -10800), then value is zero.
        /// Change to time_t; change constants above; and recompile the sources if you need to support time after 2105 year.
        UInt32 date;

        /// Properties of the day.
        UInt16 year;
        UInt8 month;
        UInt8 day_of_month;
        UInt8 day_of_week;

        /// Total number of days in current month. Actually we can use separate table that is independent of time zone.
        /// But due to alignment, this field is totally zero cost.
        UInt8 days_in_month;

        /// For days, when offset from UTC was changed due to daylight saving time or permanent change, following values could be non zero.
        UInt16 time_at_offset_change; /// In seconds from beginning of the day. Assuming offset never changed close to the end of day (so, value < 65536).
        Int16 amount_of_offset_change; /// Usually -3600 or 3600, but look at Lord Howe Island.
    };

private:
    /// Lookup table is indexed by DayNum.
    /// Day nums are the same in all time zones. 1970-01-01 is 0 and so on.
    /// Table is relatively large, so better not to place the object on stack.
    /// In comparison to std::vector, plain array is cheaper by one indirection.
    Values lut[DATE_LUT_SIZE];

    /// Year number after DATE_LUT_MIN_YEAR -> day num for start of year.
    DayNum years_lut[DATE_LUT_YEARS];

    /// Year number after DATE_LUT_MIN_YEAR * month number starting at zero -> day num for first day of month
    DayNum years_months_lut[DATE_LUT_YEARS * 12];

    /// UTC offset at beginning of the Unix epoch. The same as unix timestamp of 1970-01-01 00:00:00 local time.
    time_t offset_at_start_of_epoch;
    bool offset_is_whole_number_of_hours_everytime;

    /// Time zone name.
    std::string time_zone;


    /// We can correctly process only timestamps that less DATE_LUT_MAX (i.e. up to 2105 year inclusively)
    inline size_t findIndex(time_t t) const
    {
        /// First guess.
        size_t guess = t / 86400;
        if (guess >= DATE_LUT_MAX_DAY_NUM)
            return 0;
        if (t >= lut[guess].date && t < lut[guess + 1].date)
            return guess;

        for (size_t i = 1;; ++i)
        {
            if (guess + i >= DATE_LUT_MAX_DAY_NUM)
                return 0;
            if (t >= lut[guess + i].date && t < lut[guess + i + 1].date)
                return guess + i;
            if (guess < i)
                return 0;
            if (t >= lut[guess - i].date && t < lut[guess - i + 1].date)
                return guess - i;
        }
    }

    inline const Values & find(time_t t) const
    {
        return lut[findIndex(t)];
    }

public:
    const std::string & getTimeZone() const { return time_zone; }

    /// All functions below are thread-safe; arguments are not checked.

    inline time_t toDate(time_t t) const { return find(t).date; }
    inline unsigned toMonth(time_t t) const { return find(t).month; }
    inline unsigned toQuarter(time_t t) const { return (find(t).month - 1) / 3 + 1; }
    inline unsigned toYear(time_t t) const { return find(t).year; }
    inline unsigned toDayOfWeek(time_t t) const { return find(t).day_of_week; }
    inline unsigned toDayOfMonth(time_t t) const { return find(t).day_of_month; }

    /// Round down to start of monday.
    inline time_t toFirstDayOfWeek(time_t t) const
    {
        size_t index = findIndex(t);
        return lut[index - (lut[index].day_of_week - 1)].date;
    }

    inline DayNum toFirstDayNumOfWeek(DayNum d) const
    {
        return DayNum(d - (lut[d].day_of_week - 1));
    }

    inline DayNum toFirstDayNumOfWeek(time_t t) const
    {
        return toFirstDayNumOfWeek(toDayNum(t));
    }

    /// Round down to start of month.
    inline time_t toFirstDayOfMonth(time_t t) const
    {
        size_t index = findIndex(t);
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    inline DayNum toFirstDayNumOfMonth(DayNum d) const
    {
        return DayNum(d - (lut[d].day_of_month - 1));
    }

    inline DayNum toFirstDayNumOfMonth(time_t t) const
    {
        return toFirstDayNumOfMonth(toDayNum(t));
    }

    /// Round down to start of quarter.
    inline DayNum toFirstDayNumOfQuarter(DayNum d) const
    {
        size_t index = d;
        size_t month_inside_quarter = (lut[index].month - 1) % 3;

        index = index - lut[index].day_of_month;
        while (month_inside_quarter)
        {
            index = index - lut[index].day_of_month;
            --month_inside_quarter;
        }

        return DayNum(index + 1);
    }

    inline DayNum toFirstDayNumOfQuarter(time_t t) const
    {
        return toFirstDayNumOfQuarter(toDayNum(t));
    }

    inline time_t toFirstDayOfQuarter(time_t t) const
    {
        return fromDayNum(toFirstDayNumOfQuarter(t));
    }

    /// Round down to start of year.
    inline time_t toFirstDayOfYear(time_t t) const
    {
        return lut[years_lut[lut[findIndex(t)].year - DATE_LUT_MIN_YEAR]].date;
    }

    inline DayNum toFirstDayNumOfYear(DayNum d) const
    {
        return years_lut[lut[d].year - DATE_LUT_MIN_YEAR];
    }

    inline DayNum toFirstDayNumOfYear(time_t t) const
    {
        return toFirstDayNumOfYear(toDayNum(t));
    }

    inline time_t toFirstDayOfNextMonth(time_t t) const
    {
        size_t index = findIndex(t);
        index += 32 - lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    inline time_t toFirstDayOfPrevMonth(time_t t) const
    {
        size_t index = findIndex(t);
        index -= lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    inline UInt8 daysInMonth(DayNum d) const
    {
        return lut[d].days_in_month;
    }

    inline UInt8 daysInMonth(time_t t) const
    {
        return find(t).days_in_month;
    }

    inline UInt8 daysInMonth(UInt16 year, UInt8 month) const
    {
        /// 32 makes arithmetic more simple.
        auto any_day_of_month = years_lut[year - DATE_LUT_MIN_YEAR] + 32 * (month - 1);
        return lut[any_day_of_month].days_in_month;
    }

    /** Round to start of day, then shift for specified amount of days.
      */
    inline time_t toDateAndShift(time_t t, Int32 days) const
    {
        return lut[findIndex(t) + days].date;
    }

    inline time_t toTime(time_t t) const
    {
        size_t index = findIndex(t);

        if (unlikely(index == 0))
            return t + offset_at_start_of_epoch;

        time_t res = t - lut[index].date;

        if (res >= lut[index].time_at_offset_change)
            res += lut[index].amount_of_offset_change;

        return res - offset_at_start_of_epoch; /// Starting at 1970-01-01 00:00:00 local time.
    }

    inline unsigned toHour(time_t t) const
    {
        size_t index = findIndex(t);

        /// If it is not 1970 year (findIndex found nothing appropriate),
        ///  than limit number of hours to avoid insane results like 1970-01-01 89:28:15
        if (unlikely(index == 0))
            return static_cast<unsigned>((t + offset_at_start_of_epoch) / 3600) % 24;

        time_t res = t - lut[index].date;

        if (res >= lut[index].time_at_offset_change)
            res += lut[index].amount_of_offset_change;

        return res / 3600;
    }

    /** Only for time zones with/when offset from UTC is multiple of five minutes.
      * This is true for all time zones: right now, all time zones have an offset that is multiple of 15 minutes.
      *
      * "By 1929, most major countries had adopted hourly time zones. Nepal was the last
      *  country to adopt a standard offset, shifting slightly to UTC+5:45 in 1986."
      * - https://en.wikipedia.org/wiki/Time_zone#Offsets_from_UTC
      *
      * Also please note, that unix timestamp doesn't count "leap seconds":
      *  each minute, with added or subtracted leap second, spans exactly 60 unix timestamps.
      */

    inline unsigned toSecond(time_t t) const { return t % 60; }

    inline unsigned toMinute(time_t t) const
    {
        if (offset_is_whole_number_of_hours_everytime)
            return (t / 60) % 60;

        time_t date = find(t).date;
        return (t - date) / 60 % 60;
    }

    inline time_t toStartOfMinute(time_t t) const { return t / 60 * 60; }
    inline time_t toStartOfFiveMinute(time_t t) const { return t / 300 * 300; }
    inline time_t toStartOfFifteenMinutes(time_t t) const { return t / 900 * 900; }

    inline time_t toStartOfHour(time_t t) const
    {
        if (offset_is_whole_number_of_hours_everytime)
            return t / 3600 * 3600;

        time_t date = find(t).date;
        /// Still can return wrong values for time at 1970-01-01 if the UTC offset was non-whole number of hours.
        return date + (t - date) / 3600 * 3600;
    }

    /** Number of calendar day since the beginning of UNIX epoch (1970-01-01 is zero)
      * We use just two bytes for it. It covers the range up to 2105 and slightly more.
      *
      * This is "calendar" day, it itself is independent of time zone
      * (conversion from/to unix timestamp will depend on time zone,
      *  because the same calendar day starts/ends at different timestamps in different time zones)
      */

    inline DayNum toDayNum(time_t t) const { return static_cast<DayNum>(findIndex(t)); }
    inline time_t fromDayNum(DayNum d) const { return lut[d].date; }

    inline time_t toDate(DayNum d) const { return lut[d].date; }
    inline unsigned toMonth(DayNum d) const { return lut[d].month; }
    inline unsigned toQuarter(DayNum d) const { return (lut[d].month - 1) / 3 + 1; }
    inline unsigned toYear(DayNum d) const { return lut[d].year; }
    inline unsigned toDayOfWeek(DayNum d) const { return lut[d].day_of_week; }
    inline unsigned toDayOfMonth(DayNum d) const { return lut[d].day_of_month; }

    /// Number of week from some fixed moment in the past. Week begins at monday.
    /// (round down to monday and divide DayNum by 7; we made an assumption,
    ///  that in domain of the function there was no weeks with any other number of days than 7)
    inline unsigned toRelativeWeekNum(DayNum d) const
    {
        /// We add 8 to avoid underflow at beginning of unix epoch.
        return (d + 8 - lut[d].day_of_week) / 7;
    }

    inline unsigned toRelativeWeekNum(time_t t) const
    {
        return toRelativeWeekNum(toDayNum(t));
    }

    /// Number of month from some fixed moment in the past (year * 12 + month)
    inline unsigned toRelativeMonthNum(DayNum d) const
    {
        return lut[d].year * 12 + lut[d].month;
    }

    inline unsigned toRelativeMonthNum(time_t t) const
    {
        return toRelativeMonthNum(toDayNum(t));
    }

    inline unsigned toRelativeQuarterNum(DayNum d) const
    {
        return lut[d].year * 4 + (lut[d].month - 1) / 3;
    }

    inline unsigned toRelativeQuarterNum(time_t t) const
    {
        return toRelativeQuarterNum(toDayNum(t));
    }

    /// We count all hour-length intervals, unrelated to offset changes.
    inline time_t toRelativeHourNum(time_t t) const
    {
        if (offset_is_whole_number_of_hours_everytime)
            return t / 3600;

        /// Assume that if offset was fractional, then the fraction is the same as at the beginning of epoch.
        /// NOTE This assumption is false for "Pacific/Pitcairn" time zone.
        return (t + 86400 - offset_at_start_of_epoch) / 3600;
    }

    inline time_t toRelativeHourNum(DayNum d) const
    {
        return toRelativeHourNum(lut[d].date);
    }

    inline time_t toRelativeMinuteNum(time_t t) const
    {
        return t / 60;
    }

    inline time_t toRelativeMinuteNum(DayNum d) const
    {
        return toRelativeMinuteNum(lut[d].date);
    }

    /// Create DayNum from year, month, day of month.
    inline DayNum makeDayNum(UInt16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return DayNum(0);

        return DayNum(years_months_lut[(year - DATE_LUT_MIN_YEAR) * 12 + month - 1] + day_of_month - 1);
    }

    inline time_t makeDate(UInt16 year, UInt8 month, UInt8 day_of_month) const
    {
        return lut[makeDayNum(year, month, day_of_month)].date;
    }

    /** Does not accept daylight saving time as argument: in case of ambiguity, it choose greater timestamp.
      */
    inline time_t makeDateTime(UInt16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const
    {
        size_t index = makeDayNum(year, month, day_of_month);
        time_t time_offset = hour * 3600 + minute * 60 + second;

        if (time_offset >= lut[index].time_at_offset_change)
            time_offset -= lut[index].amount_of_offset_change;

        return lut[index].date + time_offset;
    }

    inline const Values & getValues(DayNum d) const { return lut[d]; }
    inline const Values & getValues(time_t t) const { return lut[findIndex(t)]; }

    inline UInt32 toNumYYYYMM(time_t t) const
    {
        const Values & values = find(t);
        return values.year * 100 + values.month;
    }

    inline UInt32 toNumYYYYMM(DayNum d) const
    {
        const Values & values = lut[d];
        return values.year * 100 + values.month;
    }

    inline UInt32 toNumYYYYMMDD(time_t t) const
    {
        const Values & values = find(t);
        return values.year * 10000 + values.month * 100 + values.day_of_month;
    }

    inline UInt32 toNumYYYYMMDD(DayNum d) const
    {
        const Values & values = lut[d];
        return values.year * 10000 + values.month * 100 + values.day_of_month;
    }

    inline time_t YYYYMMDDToDate(UInt32 num) const
    {
        return makeDate(num / 10000, num / 100 % 100, num % 100);
    }

    inline DayNum YYYYMMDDToDayNum(UInt32 num) const
    {
        return makeDayNum(num / 10000, num / 100 % 100, num % 100);
    }


    inline UInt64 toNumYYYYMMDDhhmmss(time_t t) const
    {
        const Values & values = find(t);
        return
              toSecond(t)
            + toMinute(t) * 100
            + toHour(t) * 10000
            + UInt64(values.day_of_month) * 1000000
            + UInt64(values.month) * 100000000
            + UInt64(values.year) * 10000000000;
    }

    inline time_t YYYYMMDDhhmmssToTime(UInt64 num) const
    {
        return makeDateTime(
            num / 10000000000,
            num / 100000000 % 100,
            num / 1000000 % 100,
            num / 10000 % 100,
            num / 100 % 100,
            num % 100);
    }

    /// Adding calendar intervals.
    /// Implementation specific behaviour when delta is too big.

    inline time_t addDays(time_t t, Int64 delta) const
    {
        size_t index = findIndex(t);
        time_t time_offset = toHour(t) * 3600 + toMinute(t) * 60 + toSecond(t);

        index += delta;

        if (time_offset >= lut[index].time_at_offset_change)
            time_offset -= lut[index].amount_of_offset_change;

        return lut[index].date + time_offset;
    }

    inline time_t addWeeks(time_t t, Int64 delta) const
    {
        return addDays(t, delta * 7);
    }

    inline UInt8 saturateDayOfMonth(UInt16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (likely(day_of_month <= 28))
            return day_of_month;

        UInt8 days_in_month = daysInMonth(year, month);

        if (day_of_month > days_in_month)
            day_of_month = days_in_month;

        return day_of_month;
    }

    /// If resulting month has less deys than source month, then saturation can happen.
    /// Example: 31 Aug + 1 month = 30 Sep.
    inline time_t addMonths(time_t t, Int64 delta) const
    {
        DayNum result_day = addMonths(toDayNum(t), delta);

        time_t time_offset = toHour(t) * 3600 + toMinute(t) * 60 + toSecond(t);

        if (time_offset >= lut[result_day].time_at_offset_change)
            time_offset -= lut[result_day].amount_of_offset_change;

        return lut[result_day].date + time_offset;
    }

    inline DayNum addMonths(DayNum d, Int64 delta) const
    {
        const Values & values = lut[d];

        Int64 month = static_cast<Int64>(values.month) + delta;

        if (month > 0)
        {
            auto year = values.year + (month - 1) / 12;
            month = ((month - 1) % 12) + 1;
            auto day_of_month = saturateDayOfMonth(year, month, values.day_of_month);

            return makeDayNum(year, month, day_of_month);
        }
        else
        {
            auto year = values.year - (12 - month) / 12;
            month = 12 - (-month % 12);
            auto day_of_month = saturateDayOfMonth(year, month, values.day_of_month);

            return makeDayNum(year, month, day_of_month);
        }
    }

    /// Saturation can occur if 29 Feb is mapped to non-leap year.
    inline time_t addYears(time_t t, Int64 delta) const
    {
        DayNum result_day = addYears(toDayNum(t), delta);

        time_t time_offset = toHour(t) * 3600 + toMinute(t) * 60 + toSecond(t);

        if (time_offset >= lut[result_day].time_at_offset_change)
            time_offset -= lut[result_day].amount_of_offset_change;

        return lut[result_day].date + time_offset;
    }

    inline DayNum addYears(DayNum d, Int64 delta) const
    {
        const Values & values = lut[d];

        auto year = values.year + delta;
        auto month = values.month;
        auto day_of_month = values.day_of_month;

        /// Saturation to 28 Feb can happen.
        if (unlikely(day_of_month == 29 && month == 2))
            day_of_month = saturateDayOfMonth(year, month, day_of_month);

        return makeDayNum(year, month, day_of_month);
    }


    inline std::string timeToString(time_t t) const
    {
        const Values & values = find(t);

        std::string s {"0000-00-00 00:00:00"};

        s[0] += values.year / 1000;
        s[1] += (values.year / 100) % 10;
        s[2] += (values.year / 10) % 10;
        s[3] += values.year % 10;
        s[5] += values.month / 10;
        s[6] += values.month % 10;
        s[8] += values.day_of_month / 10;
        s[9] += values.day_of_month % 10;

        auto hour = toHour(t);
        auto minute = toMinute(t);
        auto second = toSecond(t);

        s[11] += hour / 10;
        s[12] += hour % 10;
        s[14] += minute / 10;
        s[15] += minute % 10;
        s[17] += second / 10;
        s[18] += second % 10;

        return s;
    }

    inline std::string dateToString(time_t t) const
    {
        const Values & values = find(t);

        std::string s {"0000-00-00"};

        s[0] += values.year / 1000;
        s[1] += (values.year / 100) % 10;
        s[2] += (values.year / 10) % 10;
        s[3] += values.year % 10;
        s[5] += values.month / 10;
        s[6] += values.month % 10;
        s[8] += values.day_of_month / 10;
        s[9] += values.day_of_month % 10;

        return s;
    }

    inline std::string dateToString(DayNum d) const
    {
        const Values & values = lut[d];

        std::string s {"0000-00-00"};

        s[0] += values.year / 1000;
        s[1] += (values.year / 100) % 10;
        s[2] += (values.year / 10) % 10;
        s[3] += values.year % 10;
        s[5] += values.month / 10;
        s[6] += values.month % 10;
        s[8] += values.day_of_month / 10;
        s[9] += values.day_of_month % 10;

        return s;
    }

    inline bool isOffsetWholeNumberOfHoursEveryTime() const { return offset_is_whole_number_of_hours_everytime; }
};
