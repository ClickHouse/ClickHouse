#pragma once

#include "DayNum.h"
#include "defines.h"
#include "types.h"

#include <ctime>
#include <cassert>
#include <string>
#include <type_traits>


#define DATE_LUT_MIN_YEAR 1925 /// 1925 since wast majority of timezones changed to 15-minute aligned offsets somewhere in 1924 or earlier.
#define DATE_LUT_MAX_YEAR 2283 /// Last supported year (complete)
#define DATE_LUT_YEARS (1 + DATE_LUT_MAX_YEAR - DATE_LUT_MIN_YEAR) /// Number of years in lookup table

#define DATE_LUT_SIZE 0x20000

#define DATE_LUT_MAX (0xFFFFFFFFU - 86400)
#define DATE_LUT_MAX_DAY_NUM 0xFFFF

/// A constant to add to time_t so every supported time point becomes non-negative and still has the same remainder of division by 3600.
/// If we treat "remainder of division" operation in the sense of modular arithmetic (not like in C++).
#define DATE_LUT_ADD ((1970 - DATE_LUT_MIN_YEAR) * 366 * 86400)


#if defined(__PPC__)
#if !defined(__clang__)
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
private:
    friend class DateLUT;
    explicit DateLUTImpl(const std::string & time_zone);

    DateLUTImpl(const DateLUTImpl &) = delete;
    DateLUTImpl & operator=(const DateLUTImpl &) = delete;
    DateLUTImpl(const DateLUTImpl &&) = delete;
    DateLUTImpl & operator=(const DateLUTImpl &&) = delete;

    // Normalized and bound-checked index of element in lut,
    // has to be a separate type to support overloading
    // TODO: make sure that any arithmetic on LUTIndex actually results in valid LUTIndex.
    STRONG_TYPEDEF(UInt32, LUTIndex)

    template <typename T>
    friend inline LUTIndex operator+(const LUTIndex & index, const T v)
    {
        return LUTIndex{(index.toUnderType() + UInt32(v)) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator+(const T v, const LUTIndex & index)
    {
        return LUTIndex{(v + index.toUnderType()) & date_lut_mask};
    }

    friend inline LUTIndex operator+(const LUTIndex & index, const LUTIndex & v)
    {
        return LUTIndex{(index.toUnderType() + v.toUnderType()) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator-(const LUTIndex & index, const T v)
    {
        return LUTIndex{(index.toUnderType() - UInt32(v)) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator-(const T v, const LUTIndex & index)
    {
        return LUTIndex{(v - index.toUnderType()) & date_lut_mask};
    }

    friend inline LUTIndex operator-(const LUTIndex & index, const LUTIndex & v)
    {
        return LUTIndex{(index.toUnderType() - v.toUnderType()) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator*(const LUTIndex & index, const T v)
    {
        return LUTIndex{(index.toUnderType() * UInt32(v)) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator*(const T v, const LUTIndex & index)
    {
        return LUTIndex{(v * index.toUnderType()) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator/(const LUTIndex & index, const T v)
    {
        return LUTIndex{(index.toUnderType() / UInt32(v)) & date_lut_mask};
    }

    template <typename T>
    friend inline LUTIndex operator/(const T v, const LUTIndex & index)
    {
        return LUTIndex{(UInt32(v) / index.toUnderType()) & date_lut_mask};
    }

public:
    /// The order of fields matters for alignment and sizeof.
    struct Values
    {
        /// time_t at beginning of the day.
        Int64 date;

        /// Properties of the day.
        UInt16 year;
        UInt8 month;
        UInt8 day_of_month;
        UInt8 day_of_week;

        /// Total number of days in current month. Actually we can use separate table that is independent of time zone.
        /// But due to alignment, this field is totally zero cost.
        UInt8 days_in_month;

        /// For days, when offset from UTC was changed due to daylight saving time or permanent change, following values could be non zero.
        /// All in OffsetChangeFactor (15 minute) intervals.
        Int8 amount_of_offset_change_value; /// Usually -4 or 4, but look at Lord Howe Island. Multiply by OffsetChangeFactor
        UInt8 time_at_offset_change_value; /// In seconds from beginning of the day. Multiply by OffsetChangeFactor

        inline Int32 amount_of_offset_change() const
        {
            return static_cast<Int32>(amount_of_offset_change_value) * OffsetChangeFactor;
        }

        inline UInt32 time_at_offset_change() const
        {
            return static_cast<UInt32>(time_at_offset_change_value) * OffsetChangeFactor;
        }

        /// Since most of the modern timezones have a DST change aligned to 15 minutes, to save as much space as possible inside Value,
        /// we are dividing any offset change related value by this factor before setting it to Value,
        /// hence it has to be explicitly multiplied back by this factor before being used.
        static constexpr UInt16 OffsetChangeFactor = 900;
    };

    static_assert(sizeof(Values) == 16);

private:

    /// Mask is all-ones to allow efficient protection against overflow.
    static constexpr UInt32 date_lut_mask = 0x1ffff;
    static_assert(date_lut_mask == DATE_LUT_SIZE - 1);

    /// Offset to epoch in days (ExtendedDayNum) of the first day in LUT.
    /// "epoch" is the Unix Epoch (starts at unix timestamp zero)
    static constexpr UInt32 daynum_offset_epoch = 16436;
    static_assert(daynum_offset_epoch == (1970 - DATE_LUT_MIN_YEAR) * 365 + (1970 - DATE_LUT_MIN_YEAR / 4 * 4) / 4);

    /// Lookup table is indexed by LUTIndex.
    /// Day nums are the same in all time zones. 1970-01-01 is 0 and so on.
    /// Table is relatively large, so better not to place the object on stack.
    /// In comparison to std::vector, plain array is cheaper by one indirection.
    Values lut[DATE_LUT_SIZE + 1];

    /// Year number after DATE_LUT_MIN_YEAR -> LUTIndex in lut for start of year.
    LUTIndex years_lut[DATE_LUT_YEARS];

    /// Year number after DATE_LUT_MIN_YEAR * month number starting at zero -> day num for first day of month
    LUTIndex years_months_lut[DATE_LUT_YEARS * 12];

    /// UTC offset at beginning of the Unix epoch. The same as unix timestamp of 1970-01-01 00:00:00 local time.
    time_t offset_at_start_of_epoch;
    /// UTC offset at the beginning of the first supported year.
    time_t offset_at_start_of_lut;
    bool offset_is_whole_number_of_hours_during_epoch;

    /// Time zone name.
    std::string time_zone;

    inline LUTIndex findIndex(time_t t) const
    {
        /// First guess.
        Int64 guess = (t / 86400) + daynum_offset_epoch;

        /// For negative time_t the integer division was rounded up, so the guess is offset by one.
        if (unlikely(t < 0))
            --guess;

        if (guess < 0)
            return LUTIndex(0);
        if (guess >= DATE_LUT_SIZE)
            return LUTIndex(DATE_LUT_SIZE - 1);

        /// UTC offset is from -12 to +14 in all known time zones. This requires checking only three indices.

        if (t >= lut[guess].date)
        {
            if (guess + 1 >= DATE_LUT_SIZE || t < lut[guess + 1].date)
                return LUTIndex(guess);

            return LUTIndex(guess + 1);
        }

        return LUTIndex(guess ? guess - 1 : 0);
    }

    inline LUTIndex toLUTIndex(DayNum d) const
    {
        return LUTIndex{(d + daynum_offset_epoch) & date_lut_mask};
    }

    inline LUTIndex toLUTIndex(ExtendedDayNum d) const
    {
        return LUTIndex{static_cast<UInt32>(d + daynum_offset_epoch) & date_lut_mask};
    }

    inline LUTIndex toLUTIndex(time_t t) const
    {
        return findIndex(t);
    }

    inline LUTIndex toLUTIndex(LUTIndex i) const
    {
        return i;
    }

    template <typename DateOrTime>
    inline const Values & find(DateOrTime v) const
    {
        return lut[toLUTIndex(v)];
    }

    template <typename T, typename Divisor>
    static inline T roundDown(T x, Divisor divisor)
    {
        static_assert(std::is_integral_v<T> && std::is_integral_v<Divisor>);
        assert(divisor > 0);

        if (likely(x >= 0))
            return x / divisor * divisor;

        /// Integer division for negative numbers rounds them towards zero (up).
        /// We will shift the number so it will be rounded towards -inf (down).

        return (x + 1 - divisor) / divisor * divisor;
    }

public:
    const std::string & getTimeZone() const { return time_zone; }

    // Methods only for unit-testing, it makes very little sense to use it from user code.
    auto getOffsetAtStartOfEpoch() const { return offset_at_start_of_epoch; }
    auto getTimeOffsetAtStartOfLUT() const { return offset_at_start_of_lut; }

    /// All functions below are thread-safe; arguments are not checked.

    inline ExtendedDayNum toDayNum(ExtendedDayNum d) const
    {
        return d;
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toDayNum(DateOrTime v) const
    {
        return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(toLUTIndex(v).toUnderType() - daynum_offset_epoch)};
    }

    /// Round down to start of monday.
    template <typename DateOrTime>
    inline time_t toFirstDayOfWeek(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return lut[i - (lut[i].day_of_week - 1)].date;
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfWeek(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return toDayNum(i - (lut[i].day_of_week - 1));
    }

    /// Round down to start of month.
    template <typename DateOrTime>
    inline time_t toFirstDayOfMonth(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return lut[i - (lut[i].day_of_month - 1)].date;
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfMonth(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return toDayNum(i - (lut[i].day_of_month - 1));
    }

    /// Round down to start of quarter.
    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfQuarter(DateOrTime v) const
    {
        return toDayNum(toFirstDayOfQuarterIndex(v));
    }

    template <typename DateOrTime>
    inline LUTIndex toFirstDayOfQuarterIndex(DateOrTime v) const
    {
        LUTIndex index = toLUTIndex(v);
        size_t month_inside_quarter = (lut[index].month - 1) % 3;

        index -= lut[index].day_of_month;
        while (month_inside_quarter)
        {
            index -= lut[index].day_of_month;
            --month_inside_quarter;
        }

        return index + 1;
    }

    template <typename DateOrTime>
    inline time_t toFirstDayOfQuarter(DateOrTime v) const
    {
        return toDate(toFirstDayOfQuarterIndex(v));
    }

    /// Round down to start of year.
    inline time_t toFirstDayOfYear(time_t t) const
    {
        return lut[years_lut[lut[findIndex(t)].year - DATE_LUT_MIN_YEAR]].date;
    }

    template <typename DateOrTime>
    inline LUTIndex toFirstDayNumOfYearIndex(DateOrTime v) const
    {
        return years_lut[lut[toLUTIndex(v)].year - DATE_LUT_MIN_YEAR];
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfYear(DateOrTime v) const
    {
        return toDayNum(toFirstDayNumOfYearIndex(v));
    }

    inline time_t toFirstDayOfNextMonth(time_t t) const
    {
        LUTIndex index = findIndex(t);
        index += 32 - lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    inline time_t toFirstDayOfPrevMonth(time_t t) const
    {
        LUTIndex index = findIndex(t);
        index -= lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    template <typename DateOrTime>
    inline UInt8 daysInMonth(DateOrTime value) const
    {
        const LUTIndex i = toLUTIndex(value);
        return lut[i].days_in_month;
    }

    inline UInt8 daysInMonth(Int16 year, UInt8 month) const
    {
        UInt16 idx = year - DATE_LUT_MIN_YEAR;
        if (unlikely(idx >= DATE_LUT_YEARS))
            return 31;  /// Implementation specific behaviour on overflow.

        /// 32 makes arithmetic more simple.
        const auto any_day_of_month = years_lut[year - DATE_LUT_MIN_YEAR] + 32 * (month - 1);
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
        const LUTIndex index = findIndex(t);

        time_t res = t - lut[index].date;

        if (res >= lut[index].time_at_offset_change())
            res += lut[index].amount_of_offset_change();

        return res - offset_at_start_of_epoch; /// Starting at 1970-01-01 00:00:00 local time.
    }

    inline unsigned toHour(time_t t) const
    {
        const LUTIndex index = findIndex(t);

        time_t time = t - lut[index].date;

        if (time >= lut[index].time_at_offset_change())
            time += lut[index].amount_of_offset_change();

        unsigned res = time / 3600;

        /// In case time was changed backwards at the start of next day, we will repeat the hour 23.
        return res <= 23 ? res : 23;
    }

    /** Calculating offset from UTC in seconds.
      * which means Using the same literal time of "t" to get the corresponding timestamp in UTC,
      * then subtract the former from the latter to get the offset result.
      * The boundaries when meets DST(daylight saving time) change should be handled very carefully.
      */
    inline time_t timezoneOffset(time_t t) const
    {
        const LUTIndex index = findIndex(t);

        /// Calculate daylight saving offset first.
        /// Because the "amount_of_offset_change" in LUT entry only exists in the change day, it's costly to scan it from the very begin.
        /// but we can figure out all the accumulated offsets from 1970-01-01 to that day just by get the whole difference between lut[].date,
        /// and then, we can directly subtract multiple 86400s to get the real DST offsets for the leap seconds is not considered now.
        time_t res = (lut[index].date - lut[daynum_offset_epoch].date) % 86400;

        /// As so far to know, the maximal DST offset couldn't be more than 2 hours, so after the modulo operation the remainder
        /// will sits between [-offset --> 0 --> offset] which respectively corresponds to moving clock forward or backward.
        res = res > 43200 ? (86400 - res) : (0 - res);

        /// Check if has a offset change during this day. Add the change when cross the line
        if (lut[index].amount_of_offset_change() != 0 && t >= lut[index].date + lut[index].time_at_offset_change())
            res += lut[index].amount_of_offset_change();

        return res + offset_at_start_of_epoch;
    }


    inline unsigned toSecond(time_t t) const
    {
        auto res = t % 60;
        if (likely(res >= 0))
            return res;
        return res + 60;
    }

    inline unsigned toMinute(time_t t) const
    {
        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return (t / 60) % 60;

        /// To consider the DST changing situation within this day
        /// also make the special timezones with no whole hour offset such as 'Australia/Lord_Howe' been taken into account.

        LUTIndex index = findIndex(t);
        UInt32 time = t - lut[index].date;

        if (time >= lut[index].time_at_offset_change())
            time += lut[index].amount_of_offset_change();

        return time / 60 % 60;
    }

    /// NOTE: Assuming timezone offset is a multiple of 15 minutes.
    inline time_t toStartOfMinute(time_t t) const { return roundDown(t, 60); }
    inline time_t toStartOfFiveMinute(time_t t) const { return roundDown(t, 300); }
    inline time_t toStartOfFifteenMinutes(time_t t) const { return roundDown(t, 900); }

    inline time_t toStartOfTenMinutes(time_t t) const
    {
        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return t / 600 * 600;

        /// More complex logic is for Nepal - it has offset 05:45. Australia/Eucla is also unfortunate.
        Int64 date = find(t).date;
        return date + (t - date) / 600 * 600;
    }

    /// NOTE: Assuming timezone transitions are multiple of hours. Lord Howe Island in Australia is a notable exception.
    inline time_t toStartOfHour(time_t t) const
    {
        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return t / 3600 * 3600;

        Int64 date = find(t).date;
        return date + (t - date) / 3600 * 3600;
    }

    /** Number of calendar day since the beginning of UNIX epoch (1970-01-01 is zero)
      * We use just two bytes for it. It covers the range up to 2105 and slightly more.
      *
      * This is "calendar" day, it itself is independent of time zone
      * (conversion from/to unix timestamp will depend on time zone,
      *  because the same calendar day starts/ends at different timestamps in different time zones)
      */

    inline time_t fromDayNum(DayNum d) const { return lut[toLUTIndex(d)].date; }
    inline time_t fromDayNum(ExtendedDayNum d) const { return lut[toLUTIndex(d)].date; }

    template <typename DateOrTime>
    inline time_t toDate(DateOrTime v) const { return lut[toLUTIndex(v)].date; }

    template <typename DateOrTime>
    inline unsigned toMonth(DateOrTime v) const { return lut[toLUTIndex(v)].month; }

    template <typename DateOrTime>
    inline unsigned toQuarter(DateOrTime v) const { return (lut[toLUTIndex(v)].month - 1) / 3 + 1; }

    template <typename DateOrTime>
    inline Int16 toYear(DateOrTime v) const { return lut[toLUTIndex(v)].year; }

    template <typename DateOrTime>
    inline unsigned toDayOfWeek(DateOrTime v) const { return lut[toLUTIndex(v)].day_of_week; }

    template <typename DateOrTime>
    inline unsigned toDayOfMonth(DateOrTime v) const { return lut[toLUTIndex(v)].day_of_month; }

    template <typename DateOrTime>
    inline unsigned toDayOfYear(DateOrTime v) const
    {
        // TODO: different overload for ExtendedDayNum
        const LUTIndex i = toLUTIndex(v);
        return i + 1 - toFirstDayNumOfYearIndex(i);
    }

    /// Number of week from some fixed moment in the past. Week begins at monday.
    /// (round down to monday and divide DayNum by 7; we made an assumption,
    ///  that in domain of the function there was no weeks with any other number of days than 7)
    template <typename DateOrTime>
    inline unsigned toRelativeWeekNum(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        /// We add 8 to avoid underflow at beginning of unix epoch.
        return toDayNum(i + 8 - toDayOfWeek(i)) / 7;
    }

    /// Get year that contains most of the current week. Week begins at monday.
    template <typename DateOrTime>
    inline unsigned toISOYear(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        /// That's effectively the year of thursday of current week.
        return toYear(toLUTIndex(i + 4 - toDayOfWeek(i)));
    }

    /// ISO year begins with a monday of the week that is contained more than by half in the corresponding calendar year.
    /// Example: ISO year 2019 begins at 2018-12-31. And ISO year 2017 begins at 2017-01-02.
    /// https://en.wikipedia.org/wiki/ISO_week_date
    template <typename DateOrTime>
    inline LUTIndex toFirstDayNumOfISOYearIndex(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        auto iso_year = toISOYear(i);

        const auto first_day_of_year = years_lut[iso_year - DATE_LUT_MIN_YEAR];
        auto first_day_of_week_of_year = lut[first_day_of_year].day_of_week;

        return LUTIndex{first_day_of_week_of_year <= 4
            ? first_day_of_year + 1 - first_day_of_week_of_year
            : first_day_of_year + 8 - first_day_of_week_of_year};
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfISOYear(DateOrTime v) const
    {
        return toDayNum(toFirstDayNumOfISOYearIndex(v));
    }

    inline time_t toFirstDayOfISOYear(time_t t) const
    {
        return lut[toFirstDayNumOfISOYearIndex(t)].date;
    }

    /// ISO 8601 week number. Week begins at monday.
    /// The week number 1 is the first week in year that contains 4 or more days (that's more than half).
    template <typename DateOrTime>
    inline unsigned toISOWeek(DateOrTime v) const
    {
        return 1 + (toFirstDayNumOfWeek(v) - toFirstDayNumOfISOYear(v)) / 7;
    }

    /*
      The bits in week_mode has the following meaning:
       WeekModeFlag::MONDAY_FIRST (0)  If not set Sunday is first day of week
                      If set Monday is first day of week
       WeekModeFlag::YEAR (1) If not set Week is in range 0-53

        Week 0 is returned for the the last week of the previous year (for
        a date at start of january) In this case one can get 53 for the
        first week of next year.  This flag ensures that the week is
        relevant for the given year. Note that this flag is only
        relevant if WeekModeFlag::JANUARY is not set.

                  If set Week is in range 1-53.

        In this case one may get week 53 for a date in January (when
        the week is that last week of previous year) and week 1 for a
        date in December.

      WeekModeFlag::FIRST_WEEKDAY (2) If not set Weeks are numbered according
                        to ISO 8601:1988
                  If set The week that contains the first
                        'first-day-of-week' is week 1.

      WeekModeFlag::NEWYEAR_DAY (3) If not set no meaning
                  If set The week that contains the January 1 is week 1.
                            Week is in range 1-53.
                            And ignore WeekModeFlag::YEAR, WeekModeFlag::FIRST_WEEKDAY

        ISO 8601:1988 means that if the week containing January 1 has
        four or more days in the new year, then it is week 1;
        Otherwise it is the last week of the previous year, and the
        next week is week 1.
    */
    template <typename DateOrTime>
    inline YearWeek toYearWeek(DateOrTime v, UInt8 week_mode) const
    {
        const bool newyear_day_mode = week_mode & static_cast<UInt8>(WeekModeFlag::NEWYEAR_DAY);
        week_mode = check_week_mode(week_mode);
        const bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        bool week_year_mode = week_mode & static_cast<UInt8>(WeekModeFlag::YEAR);
        const bool first_weekday_mode = week_mode & static_cast<UInt8>(WeekModeFlag::FIRST_WEEKDAY);

        const LUTIndex i = toLUTIndex(v);

        // Calculate week number of WeekModeFlag::NEWYEAR_DAY mode
        if (newyear_day_mode)
        {
            return toYearWeekOfNewyearMode(i, monday_first_mode);
        }

        YearWeek yw(toYear(i), 0);
        UInt16 days = 0;
        const auto daynr = makeDayNum(yw.first, toMonth(i), toDayOfMonth(i));
        auto first_daynr = makeDayNum(yw.first, 1, 1);

        // 0 for monday, 1 for tuesday ...
        // get weekday from first day in year.
        UInt16 weekday = calc_weekday(first_daynr, !monday_first_mode);

        if (toMonth(i) == 1 && toDayOfMonth(i) <= static_cast<UInt32>(7 - weekday))
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
    template <typename DateOrTime>
    inline YearWeek toYearWeekOfNewyearMode(DateOrTime v, bool monday_first_mode) const
    {
        YearWeek yw(0, 0);
        UInt16 offset_day = monday_first_mode ? 0U : 1U;

        const LUTIndex i = LUTIndex(v);

        // Checking the week across the year
        yw.first = toYear(i + 7 - toDayOfWeek(i + offset_day));

        auto first_day = makeLUTIndex(yw.first, 1, 1);
        auto this_day = i;

        // TODO: do not perform calculations in terms of DayNum, since that would under/overflow for extended range.
        if (monday_first_mode)
        {
            // Rounds down a date to the nearest Monday.
            first_day = toFirstDayNumOfWeek(first_day);
            this_day = toFirstDayNumOfWeek(i);
        }
        else
        {
            // Rounds down a date to the nearest Sunday.
            if (toDayOfWeek(first_day) != 7)
                first_day = ExtendedDayNum(first_day - toDayOfWeek(first_day));
            if (toDayOfWeek(i) != 7)
                this_day = ExtendedDayNum(i - toDayOfWeek(i));
        }
        yw.second = (this_day - first_day) / 7 + 1;
        return yw;
    }

    /// Get first day of week with week_mode, return Sunday or Monday
    template <typename DateOrTime>
    inline ExtendedDayNum toFirstDayNumOfWeek(DateOrTime v, UInt8 week_mode) const
    {
        bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        if (monday_first_mode)
        {
            return toFirstDayNumOfWeek(v);
        }
        else
        {
            return (toDayOfWeek(v) != 7) ? ExtendedDayNum(v - toDayOfWeek(v)) : toDayNum(v);
        }
    }

    /// Check and change mode to effective.
    inline UInt8 check_week_mode(UInt8 mode) const
    {
        UInt8 week_format = (mode & 7);
        if (!(week_format & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST)))
            week_format ^= static_cast<UInt8>(WeekModeFlag::FIRST_WEEKDAY);
        return week_format;
    }

    /** Calculate weekday from d.
      * Returns 0 for monday, 1 for tuesday...
      */
    template <typename DateOrTime>
    inline unsigned calc_weekday(DateOrTime v, bool sunday_first_day_of_week) const
    {
        const LUTIndex i = toLUTIndex(v);
        if (!sunday_first_day_of_week)
            return toDayOfWeek(i) - 1;
        else
            return toDayOfWeek(i + 1) - 1;
    }

    /// Calculate days in one year.
    inline unsigned calc_days_in_year(Int32 year) const
    {
        return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? 366 : 365);
    }

    /// Number of month from some fixed moment in the past (year * 12 + month)
    template <typename DateOrTime>
    inline unsigned toRelativeMonthNum(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return lut[i].year * 12 + lut[i].month;
    }

    template <typename DateOrTime>
    inline unsigned toRelativeQuarterNum(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        return lut[i].year * 4 + (lut[i].month - 1) / 3;
    }

    /// We count all hour-length intervals, unrelated to offset changes.
    inline time_t toRelativeHourNum(time_t t) const
    {
        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return t / 3600;

        /// Assume that if offset was fractional, then the fraction is the same as at the beginning of epoch.
        /// NOTE This assumption is false for "Pacific/Pitcairn" and "Pacific/Kiritimati" time zones.
        return (t + DATE_LUT_ADD + 86400 - offset_at_start_of_epoch) / 3600 - (DATE_LUT_ADD / 3600);
    }

    template <typename DateOrTime>
    inline time_t toRelativeHourNum(DateOrTime v) const
    {
        return toRelativeHourNum(lut[toLUTIndex(v)].date);
    }

    inline time_t toRelativeMinuteNum(time_t t) const
    {
        return (t + DATE_LUT_ADD) / 60 - (DATE_LUT_ADD / 60);
    }

    template <typename DateOrTime>
    inline time_t toRelativeMinuteNum(DateOrTime v) const
    {
        return toRelativeMinuteNum(lut[toLUTIndex(v)].date);
    }

    template <typename DateOrTime>
    inline ExtendedDayNum toStartOfYearInterval(DateOrTime v, UInt64 years) const
    {
        if (years == 1)
            return toFirstDayNumOfYear(v);

        const LUTIndex i = toLUTIndex(v);

        UInt16 year = lut[i].year / years * years;

        /// For example, rounding down 1925 to 100 years will be 1900, but it's less than min supported year.
        if (unlikely(year < DATE_LUT_MIN_YEAR))
            year = DATE_LUT_MIN_YEAR;

        return toDayNum(years_lut[year - DATE_LUT_MIN_YEAR]);
    }

    inline ExtendedDayNum toStartOfQuarterInterval(ExtendedDayNum d, UInt64 quarters) const
    {
        if (quarters == 1)
            return toFirstDayNumOfQuarter(d);
        return toStartOfMonthInterval(d, quarters * 3);
    }

    inline ExtendedDayNum toStartOfMonthInterval(ExtendedDayNum d, UInt64 months) const
    {
        if (months == 1)
            return toFirstDayNumOfMonth(d);
        const Values & values = lut[toLUTIndex(d)];
        UInt32 month_total_index = (values.year - DATE_LUT_MIN_YEAR) * 12 + values.month - 1;
        return toDayNum(years_months_lut[month_total_index / months * months]);
    }

    inline ExtendedDayNum toStartOfWeekInterval(ExtendedDayNum d, UInt64 weeks) const
    {
        if (weeks == 1)
            return toFirstDayNumOfWeek(d);
        UInt64 days = weeks * 7;
        // January 1st 1970 was Thursday so we need this 4-days offset to make weeks start on Monday.
        return ExtendedDayNum(4 + (d - 4) / days * days);
    }

    inline time_t toStartOfDayInterval(ExtendedDayNum d, UInt64 days) const
    {
        if (days == 1)
            return toDate(d);
        return lut[toLUTIndex(ExtendedDayNum(d / days * days))].date;
    }

    inline time_t toStartOfHourInterval(time_t t, UInt64 hours) const
    {
        if (hours == 1)
            return toStartOfHour(t);

        /** We will round the hour number since the midnight.
          * It may split the day into non-equal intervals.
          * For example, if we will round to 11-hour interval,
          * the day will be split to the intervals 00:00:00..10:59:59, 11:00:00..21:59:59, 22:00:00..23:59:59.
          * In case of daylight saving time or other transitions,
          * the intervals can be shortened or prolonged to the amount of transition.
          */

        UInt64 seconds = hours * 3600;

        const LUTIndex index = findIndex(t);
        const Values & values = lut[index];

        time_t time = t - values.date;
        if (time >= values.time_at_offset_change())
        {
            /// Align to new hour numbers before rounding.
            time += values.amount_of_offset_change();
            time = time / seconds * seconds;

            /// Should subtract the shift back but only if rounded time is not before shift.
            if (time >= values.time_at_offset_change())
            {
                time -= values.amount_of_offset_change();

                /// With cutoff at the time of the shift. Otherwise we may end up with something like 23:00 previous day.
                if (time < values.time_at_offset_change())
                    time = values.time_at_offset_change();
            }
        }
        else
        {
            time = time / seconds * seconds;
        }

        return values.date + time;
    }

    inline time_t toStartOfMinuteInterval(time_t t, UInt64 minutes) const
    {
        if (minutes == 1)
            return toStartOfMinute(t);

        /** In contrast to "toStartOfHourInterval" function above,
          * the minute intervals are not aligned to the midnight.
          * You will get unexpected results if for example, you round down to 60 minute interval
          * and there was a time shift to 30 minutes.
          *
          * But this is not specified in docs and can be changed in future.
          */

        UInt64 seconds = 60 * minutes;
        return roundDown(t, seconds);
    }

    inline time_t toStartOfSecondInterval(time_t t, UInt64 seconds) const
    {
        if (seconds == 1)
            return t;

        return roundDown(t, seconds);
    }

    inline LUTIndex makeLUTIndex(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return LUTIndex(0);

        return LUTIndex{years_months_lut[(year - DATE_LUT_MIN_YEAR) * 12 + month - 1] + day_of_month - 1};
    }

    /// Create DayNum from year, month, day of month.
    inline ExtendedDayNum makeDayNum(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return ExtendedDayNum(0);

        return toDayNum(makeLUTIndex(year, month, day_of_month));
    }

    inline time_t makeDate(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        return lut[makeLUTIndex(year, month, day_of_month)].date;
    }

    /** Does not accept daylight saving time as argument: in case of ambiguity, it choose greater timestamp.
      */
    inline time_t makeDateTime(Int16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const
    {
        size_t index = makeLUTIndex(year, month, day_of_month);
        UInt32 time_offset = hour * 3600 + minute * 60 + second;

        if (time_offset >= lut[index].time_at_offset_change())
            time_offset -= lut[index].amount_of_offset_change();

        return lut[index].date + time_offset;
    }

    template <typename DateOrTime>
    inline const Values & getValues(DateOrTime v) const { return lut[toLUTIndex(v)]; }

    template <typename DateOrTime>
    inline UInt32 toNumYYYYMM(DateOrTime v) const
    {
        const Values & values = getValues(v);
        return values.year * 100 + values.month;
    }

    template <typename DateOrTime>
    inline UInt32 toNumYYYYMMDD(DateOrTime v) const
    {
        const Values & values = getValues(v);
        return values.year * 10000 + values.month * 100 + values.day_of_month;
    }

    inline time_t YYYYMMDDToDate(UInt32 num) const
    {
        return makeDate(num / 10000, num / 100 % 100, num % 100);
    }

    inline ExtendedDayNum YYYYMMDDToDayNum(UInt32 num) const
    {
        return makeDayNum(num / 10000, num / 100 % 100, num % 100);
    }


    struct DateComponents
    {
        uint16_t year;
        uint8_t month;
        uint8_t day;
    };

    struct TimeComponents
    {
        uint8_t hour;
        uint8_t minute;
        uint8_t second;
    };

    struct DateTimeComponents
    {
        DateComponents date;
        TimeComponents time;
    };

    inline DateComponents toDateComponents(time_t t) const
    {
        const Values & values = getValues(t);
        return { values.year, values.month, values.day_of_month };
    }

    inline DateTimeComponents toDateTimeComponents(time_t t) const
    {
        const LUTIndex index = findIndex(t);
        const Values & values = lut[index];

        DateTimeComponents res;

        res.date.year = values.year;
        res.date.month = values.month;
        res.date.day = values.day_of_month;

        time_t time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        if (unlikely(time < 0))
        {
            res.time.second = 0;
            res.time.minute = 0;
            res.time.hour = 0;
        }
        else
        {
            res.time.second = time % 60;
            res.time.minute = time / 60 % 60;
            res.time.hour = time / 3600;
        }

        /// In case time was changed backwards at the start of next day, we will repeat the hour 23.
        if (unlikely(res.time.hour > 23))
            res.time.hour = 23;

        return res;
    }


    inline UInt64 toNumYYYYMMDDhhmmss(time_t t) const
    {
        DateTimeComponents components = toDateTimeComponents(t);

        return
              components.time.second
            + components.time.minute * 100
            + components.time.hour * 10000
            + UInt64(components.date.day) * 1000000
            + UInt64(components.date.month) * 100000000
            + UInt64(components.date.year) * 10000000000;
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

    inline NO_SANITIZE_UNDEFINED time_t addDays(time_t t, Int64 delta) const
    {
        const LUTIndex index = findIndex(t);
        const Values & values = lut[index];

        time_t time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        const LUTIndex new_index = index + delta;

        if (time >= lut[new_index].time_at_offset_change())
            time -= lut[new_index].amount_of_offset_change();

        return lut[new_index].date + time;
    }

    inline NO_SANITIZE_UNDEFINED time_t addWeeks(time_t t, Int64 delta) const
    {
        return addDays(t, delta * 7);
    }

    inline UInt8 saturateDayOfMonth(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (likely(day_of_month <= 28))
            return day_of_month;

        UInt8 days_in_month = daysInMonth(year, month);

        if (day_of_month > days_in_month)
            day_of_month = days_in_month;

        return day_of_month;
    }

    template <typename DateOrTime>
    inline LUTIndex NO_SANITIZE_UNDEFINED addMonthsIndex(DateOrTime v, Int64 delta) const
    {
        const Values & values = lut[toLUTIndex(v)];

        Int64 month = values.month + delta;

        if (month > 0)
        {
            auto year = values.year + (month - 1) / 12;
            month = ((month - 1) % 12) + 1;
            auto day_of_month = saturateDayOfMonth(year, month, values.day_of_month);

            return makeLUTIndex(year, month, day_of_month);
        }
        else
        {
            auto year = values.year - (12 - month) / 12;
            month = 12 - (-month % 12);
            auto day_of_month = saturateDayOfMonth(year, month, values.day_of_month);

            return makeLUTIndex(year, month, day_of_month);
        }
    }

    /// If resulting month has less deys than source month, then saturation can happen.
    /// Example: 31 Aug + 1 month = 30 Sep.
    inline time_t NO_SANITIZE_UNDEFINED addMonths(time_t t, Int64 delta) const
    {
        const auto result_day = addMonthsIndex(t, delta);

        const LUTIndex index = findIndex(t);
        const Values & values = lut[index];

        time_t time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        if (time >= lut[result_day].time_at_offset_change())
            time -= lut[result_day].amount_of_offset_change();

        return lut[result_day].date + time;
    }

    inline ExtendedDayNum NO_SANITIZE_UNDEFINED addMonths(ExtendedDayNum d, Int64 delta) const
    {
        return toDayNum(addMonthsIndex(d, delta));
    }

    inline time_t NO_SANITIZE_UNDEFINED addQuarters(time_t t, Int64 delta) const
    {
        return addMonths(t, delta * 3);
    }

    inline ExtendedDayNum addQuarters(ExtendedDayNum d, Int64 delta) const
    {
        return addMonths(d, delta * 3);
    }

    template <typename DateOrTime>
    inline LUTIndex NO_SANITIZE_UNDEFINED addYearsIndex(DateOrTime v, Int64 delta) const
    {
        const Values & values = lut[toLUTIndex(v)];

        auto year = values.year + delta;
        auto month = values.month;
        auto day_of_month = values.day_of_month;

        /// Saturation to 28 Feb can happen.
        if (unlikely(day_of_month == 29 && month == 2))
            day_of_month = saturateDayOfMonth(year, month, day_of_month);

        return makeLUTIndex(year, month, day_of_month);
    }

    /// Saturation can occur if 29 Feb is mapped to non-leap year.
    inline time_t addYears(time_t t, Int64 delta) const
    {
        auto result_day = addYearsIndex(t, delta);

        const LUTIndex index = findIndex(t);
        const Values & values = lut[index];

        time_t time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        if (time >= lut[result_day].time_at_offset_change())
            time -= lut[result_day].amount_of_offset_change();

        return lut[result_day].date + time;
    }

    inline ExtendedDayNum addYears(ExtendedDayNum d, Int64 delta) const
    {
        return toDayNum(addYearsIndex(d, delta));
    }


    inline std::string timeToString(time_t t) const
    {
        DateTimeComponents components = toDateTimeComponents(t);

        std::string s {"0000-00-00 00:00:00"};

        s[0] += components.date.year / 1000;
        s[1] += (components.date.year / 100) % 10;
        s[2] += (components.date.year / 10) % 10;
        s[3] += components.date.year % 10;
        s[5] += components.date.month / 10;
        s[6] += components.date.month % 10;
        s[8] += components.date.day / 10;
        s[9] += components.date.day % 10;

        s[11] += components.time.hour / 10;
        s[12] += components.time.hour % 10;
        s[14] += components.time.minute / 10;
        s[15] += components.time.minute % 10;
        s[17] += components.time.second / 10;
        s[18] += components.time.second % 10;

        return s;
    }

    inline std::string dateToString(time_t t) const
    {
        const Values & values = getValues(t);

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

    inline std::string dateToString(ExtendedDayNum d) const
    {
        const Values & values = getValues(d);

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
#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
#endif
