#pragma once

#include "Types.h"
#include "DayNum.h"
#include "likely.h"
#include <ctime>
#include <string>

#define DATE_LUT_MAX (0xFFFFFFFFU - 86400)
#define DATE_LUT_MAX_DAY_NUM (0xFFFFFFFFU / 86400)
/// Table size is bigger than DATE_LUT_MAX_DAY_NUM to fill all indices within UInt16 range: this allows to remove extra check.
#define DATE_LUT_SIZE 0x10000
#define DATE_LUT_MIN_YEAR 1970
#define DATE_LUT_MAX_YEAR 2105 /// Last supported year
#define DATE_LUT_YEARS (1 + DATE_LUT_MAX_YEAR - DATE_LUT_MIN_YEAR) /// Number of years in lookup table

#if defined(__PPC__)
#if !__clang__
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#endif

/// Flags for toYearWeek() function.
enum class WeekModeFlag : UInt8
{
    MONDAY_FIRST = 1,
    YEAR = 2,
    FIRST_WEEKDAY = 4,
    NEWYEAR_DAY = 8
};
using YearWeek = std::pair<UInt16, UInt8>;

/** Lookup table to conversion of time to date, and to month / year / day of week / day of month and so on.
  * First time was implemented for OLAPServer, that needed to do billions of such transformations.
  */
class DateLUTImpl
{
public:
    DateLUTImpl(const std::string & time_zone);

public:
    /// The order of fields matters for alignment and sizeof.
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
        Int16 amount_of_offset_change; /// Usually -3600 or 3600, but look at Lord Howe Island.
        UInt32 time_at_offset_change; /// In seconds from beginning of the day.
    };

    static_assert(sizeof(Values) == 16);

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
    /// We don't care about overflow.
    inline DayNum findIndex(time_t t) const
    {
        /// First guess.
        DayNum guess(t / 86400);

        /// UTC offset is from -12 to +14 in all known time zones. This requires checking only three indices.

        if ((guess == 0 || t >= lut[guess].date) && t < lut[DayNum(guess + 1)].date)
            return guess;

        /// Time zones that have offset 0 from UTC do daylight saving time change (if any) towards increasing UTC offset (example: British Standard Time).
        if (offset_at_start_of_epoch >= 0)
            return DayNum(guess + 1);

        return DayNum(guess - 1);
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
        DayNum index = findIndex(t);
        return lut[DayNum(index - (lut[index].day_of_week - 1))].date;
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
        DayNum index = findIndex(t);
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
        DayNum index = d;
        size_t month_inside_quarter = (lut[index].month - 1) % 3;

        index -= lut[index].day_of_month;
        while (month_inside_quarter)
        {
            index -= lut[index].day_of_month;
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
        DayNum index = findIndex(t);
        index += 32 - lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    inline time_t toFirstDayOfPrevMonth(time_t t) const
    {
        DayNum index = findIndex(t);
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
        DayNum any_day_of_month = DayNum(years_lut[year - DATE_LUT_MIN_YEAR] + 32 * (month - 1));
        return lut[any_day_of_month].days_in_month;
    }

    /** Round to start of day, then shift for specified amount of days.
      */
    inline time_t toDateAndShift(time_t t, Int32 days) const
    {
        return lut[DayNum(findIndex(t) + days)].date;
    }

    inline time_t toTime(time_t t) const
    {
        DayNum index = findIndex(t);

        if (unlikely(index == 0))
            return t + offset_at_start_of_epoch;

        time_t res = t - lut[index].date;

        if (res >= lut[index].time_at_offset_change)
            res += lut[index].amount_of_offset_change;

        return res - offset_at_start_of_epoch; /// Starting at 1970-01-01 00:00:00 local time.
    }

    inline unsigned toHour(time_t t) const
    {
        DayNum index = findIndex(t);

        /// If it is not 1970 year (findIndex found nothing appropriate),
        ///  than limit number of hours to avoid insane results like 1970-01-01 89:28:15
        if (unlikely(index == 0))
            return static_cast<unsigned>((t + offset_at_start_of_epoch) / 3600) % 24;

        time_t res = t - lut[index].date;

        /// Data is cleaned to avoid possibility of underflow.
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
    inline time_t toStartOfTenMinutes(time_t t) const { return t / 600 * 600; }

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

    inline DayNum toDayNum(time_t t) const { return findIndex(t); }
    inline time_t fromDayNum(DayNum d) const { return lut[d].date; }

    inline time_t toDate(DayNum d) const { return lut[d].date; }
    inline unsigned toMonth(DayNum d) const { return lut[d].month; }
    inline unsigned toQuarter(DayNum d) const { return (lut[d].month - 1) / 3 + 1; }
    inline unsigned toYear(DayNum d) const { return lut[d].year; }
    inline unsigned toDayOfWeek(DayNum d) const { return lut[d].day_of_week; }
    inline unsigned toDayOfMonth(DayNum d) const { return lut[d].day_of_month; }
    inline unsigned toDayOfYear(DayNum d) const { return d + 1 - toFirstDayNumOfYear(d); }

    inline unsigned toDayOfYear(time_t t) const { return toDayOfYear(toDayNum(t)); }

    /// Number of week from some fixed moment in the past. Week begins at monday.
    /// (round down to monday and divide DayNum by 7; we made an assumption,
    ///  that in domain of the function there was no weeks with any other number of days than 7)
    inline unsigned toRelativeWeekNum(DayNum d) const
    {
        /// We add 8 to avoid underflow at beginning of unix epoch.
        return (d + 8 - toDayOfWeek(d)) / 7;
    }

    inline unsigned toRelativeWeekNum(time_t t) const
    {
        return toRelativeWeekNum(toDayNum(t));
    }

    /// Get year that contains most of the current week. Week begins at monday.
    inline unsigned toISOYear(DayNum d) const
    {
        /// That's effectively the year of thursday of current week.
        return toYear(DayNum(d + 4 - toDayOfWeek(d)));
    }

    inline unsigned toISOYear(time_t t) const
    {
        return toISOYear(toDayNum(t));
    }

    /// ISO year begins with a monday of the week that is contained more than by half in the corresponding calendar year.
    /// Example: ISO year 2019 begins at 2018-12-31. And ISO year 2017 begins at 2017-01-02.
    /// https://en.wikipedia.org/wiki/ISO_week_date
    inline DayNum toFirstDayNumOfISOYear(DayNum d) const
    {
        auto iso_year = toISOYear(d);

        DayNum first_day_of_year = years_lut[iso_year - DATE_LUT_MIN_YEAR];
        auto first_day_of_week_of_year = lut[first_day_of_year].day_of_week;

        return DayNum(first_day_of_week_of_year <= 4
            ? first_day_of_year + 1 - first_day_of_week_of_year
            : first_day_of_year + 8 - first_day_of_week_of_year);
    }

    inline DayNum toFirstDayNumOfISOYear(time_t t) const
    {
        return toFirstDayNumOfISOYear(toDayNum(t));
    }

    inline time_t toFirstDayOfISOYear(time_t t) const
    {
        return fromDayNum(toFirstDayNumOfISOYear(t));
    }

    /// ISO 8601 week number. Week begins at monday.
    /// The week number 1 is the first week in year that contains 4 or more days (that's more than half).
    inline unsigned toISOWeek(DayNum d) const
    {
        return 1 + DayNum(toFirstDayNumOfWeek(d) - toFirstDayNumOfISOYear(d)) / 7;
    }

    inline unsigned toISOWeek(time_t t) const
    {
        return toISOWeek(toDayNum(t));
    }

    /*
      The bits in week_mode has the following meaning:
       WeekModeFlag::MONDAY_FIRST (0)  If not set	Sunday is first day of week
                      If set	Monday is first day of week
       WeekModeFlag::YEAR (1)	  If not set	Week is in range 0-53

        Week 0 is returned for the the last week of the previous year (for
        a date at start of january) In this case one can get 53 for the
        first week of next year.  This flag ensures that the week is
        relevant for the given year. Note that this flag is only
        releveant if WeekModeFlag::JANUARY is not set.

                  If set	 Week is in range 1-53.

        In this case one may get week 53 for a date in January (when
        the week is that last week of previous year) and week 1 for a
        date in December.

      WeekModeFlag::FIRST_WEEKDAY (2)  If not set	Weeks are numbered according
                        to ISO 8601:1988
                  If set	The week that contains the first
                        'first-day-of-week' is week 1.

      WeekModeFlag::NEWYEAR_DAY (3)   If not set  no meaning
                  If set	The week that contains the January 1 is week 1.
                            Week is in range 1-53.
                            And ignore WeekModeFlag::YEAR, WeekModeFlag::FIRST_WEEKDAY

        ISO 8601:1988 means that if the week containing January 1 has
        four or more days in the new year, then it is week 1;
        Otherwise it is the last week of the previous year, and the
        next week is week 1.
    */
    inline YearWeek toYearWeek(DayNum d, UInt8 week_mode) const
    {
        bool newyear_day_mode = week_mode & static_cast<UInt8>(WeekModeFlag::NEWYEAR_DAY);
        week_mode = check_week_mode(week_mode);
        bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        bool week_year_mode = week_mode & static_cast<UInt8>(WeekModeFlag::YEAR);
        bool first_weekday_mode = week_mode & static_cast<UInt8>(WeekModeFlag::FIRST_WEEKDAY);

        // Calculate week number of WeekModeFlag::NEWYEAR_DAY mode
        if (newyear_day_mode)
        {
            return toYearWeekOfNewyearMode(d, monday_first_mode);
        }

        YearWeek yw(toYear(d), 0);
        UInt16 days = 0;
        UInt16 daynr = makeDayNum(yw.first, toMonth(d), toDayOfMonth(d));
        UInt16 first_daynr = makeDayNum(yw.first, 1, 1);

        // 0 for monday, 1 for tuesday ...
        // get weekday from first day in year.
        UInt16 weekday = calc_weekday(DayNum(first_daynr), !monday_first_mode);

        if (toMonth(d) == 1 && toDayOfMonth(d) <= static_cast<UInt32>(7 - weekday))
        {
            if (!week_year_mode && ((first_weekday_mode && weekday != 0) || (!first_weekday_mode && weekday >= 4)))
                return yw;
            week_year_mode = 1;
            (yw.first)--;
            first_daynr -= (days = calc_days_in_year(yw.first));
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if ((first_weekday_mode && weekday != 0) || (!first_weekday_mode && weekday >= 4))
            days = daynr - (first_daynr + (7 - weekday));
        else
            days = daynr - (first_daynr - weekday);

        if (week_year_mode && days >= 52 * 7)
        {
            weekday = (weekday + calc_days_in_year(yw.first)) % 7;
            if ((!first_weekday_mode && weekday < 4) || (first_weekday_mode && weekday == 0))
            {
                (yw.first)++;
                yw.second = 1;
                return yw;
            }
        }
        yw.second = days / 7 + 1;
        return yw;
    }

    /// Calculate week number of WeekModeFlag::NEWYEAR_DAY mode
    /// The week number 1 is the first week in year that contains January 1,
    inline YearWeek toYearWeekOfNewyearMode(DayNum d, bool monday_first_mode) const
    {
        YearWeek yw(0, 0);
        UInt16 offset_day = monday_first_mode ? 0U : 1U;

        // Checking the week across the year
        yw.first = toYear(DayNum(d + 7 - toDayOfWeek(DayNum(d + offset_day))));

        DayNum first_day = makeDayNum(yw.first, 1, 1);
        DayNum this_day = d;

        if (monday_first_mode)
        {
            // Rounds down a date to the nearest Monday.
            first_day = toFirstDayNumOfWeek(first_day);
            this_day = toFirstDayNumOfWeek(d);
        }
        else
        {
            // Rounds down a date to the nearest Sunday.
            if (toDayOfWeek(first_day) != 7)
                first_day = DayNum(first_day - toDayOfWeek(first_day));
            if (toDayOfWeek(d) != 7)
                this_day = DayNum(d - toDayOfWeek(d));
        }
        yw.second = (this_day - first_day) / 7 + 1;
        return yw;
    }

    /**
     * get first day of week with week_mode, return Sunday or Monday
     */
    inline DayNum toFirstDayNumOfWeek(DayNum d, UInt8 week_mode) const
    {
        bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        if (monday_first_mode)
        {
            return toFirstDayNumOfWeek(d);
        }
        else
        {
            return (toDayOfWeek(d) != 7) ? DayNum(d - toDayOfWeek(d)) : d;
        }
    }

    /*
     * check and change mode to effective
     */
    inline UInt8 check_week_mode(UInt8 mode) const
    {
        UInt8 week_format = (mode & 7);
        if (!(week_format & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST)))
            week_format ^= static_cast<UInt8>(WeekModeFlag::FIRST_WEEKDAY);
        return week_format;
    }

    /*
     * Calc weekday from d
     * Returns 0 for monday, 1 for tuesday ...
     */
    inline unsigned calc_weekday(DayNum d, bool sunday_first_day_of_week) const
    {
        if (!sunday_first_day_of_week)
            return toDayOfWeek(d) - 1;
        else
            return toDayOfWeek(DayNum(d + 1)) - 1;
    }

    /* Calc days in one year. */
    inline unsigned calc_days_in_year(UInt16 year) const
    {
        return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? 366 : 365);
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

    inline DayNum toStartOfYearInterval(DayNum d, UInt64 years) const
    {
        if (years == 1)
            return toFirstDayNumOfYear(d);
        return years_lut[(lut[d].year - DATE_LUT_MIN_YEAR) / years * years];
    }

    inline DayNum toStartOfQuarterInterval(DayNum d, UInt64 quarters) const
    {
        if (quarters == 1)
            return toFirstDayNumOfQuarter(d);
        return toStartOfMonthInterval(d, quarters * 3);
    }

    inline DayNum toStartOfMonthInterval(DayNum d, UInt64 months) const
    {
        if (months == 1)
            return toFirstDayNumOfMonth(d);
        const auto & date = lut[d];
        UInt32 month_total_index = (date.year - DATE_LUT_MIN_YEAR) * 12 + date.month - 1;
        return years_months_lut[month_total_index / months * months];
    }

    inline DayNum toStartOfWeekInterval(DayNum d, UInt64 weeks) const
    {
        if (weeks == 1)
            return toFirstDayNumOfWeek(d);
        UInt64 days = weeks * 7;
        // January 1st 1970 was Thursday so we need this 4-days offset to make weeks start on Monday.
        return DayNum(4 + (d - 4) / days * days);
    }

    inline time_t toStartOfDayInterval(DayNum d, UInt64 days) const
    {
        if (days == 1)
            return toDate(d);
        return lut[d / days * days].date;
    }

    inline time_t toStartOfHourInterval(time_t t, UInt64 hours) const
    {
        if (hours == 1)
            return toStartOfHour(t);
        UInt64 seconds = hours * 3600;
        t = t / seconds * seconds;
        if (offset_is_whole_number_of_hours_everytime)
            return t;
        return toStartOfHour(t);
    }

    inline time_t toStartOfMinuteInterval(time_t t, UInt64 minutes) const
    {
        if (minutes == 1)
            return toStartOfMinute(t);
        UInt64 seconds = 60 * minutes;
        return t / seconds * seconds;
    }

    inline time_t toStartOfSecondInterval(time_t t, UInt64 seconds) const
    {
        if (seconds == 1)
            return t;
        return t / seconds * seconds;
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
        DayNum index = findIndex(t);
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

    inline time_t addQuarters(time_t t, Int64 delta) const
    {
        return addMonths(t, delta * 3);
    }

    inline DayNum addQuarters(DayNum d, Int64 delta) const
    {
        return addMonths(d, delta * 3);
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
};

#if defined(__PPC__)
#if !__clang__
#pragma GCC diagnostic pop
#endif
#endif
