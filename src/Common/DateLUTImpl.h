#pragma once

#include <base/DayNum.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <ctime>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>

namespace cctz
{
class time_zone;
}

/// NOLINTBEGIN(modernize-macro-to-enum)
#define DATE_SECONDS_PER_DAY 86400 /// Number of seconds in a day, 60 * 60 * 24

#define DATE_LUT_MIN_YEAR 1900 /// 1900 since majority of financial organizations consider 1900 as an initial year.
#define DATE_LUT_MAX_YEAR 2299 /// Last year covered by the lookup table (complete)
#define DATE_LUT_YEARS (1 + DATE_LUT_MAX_YEAR - DATE_LUT_MIN_YEAR) /// Number of years in lookup table

/// Years outside the lookup table are handled with cctz. The overall representable range is bounded by the
/// 4-digit year used when formatting, i.e. [0000, 9999].
#define DATE_LUT_MIN_REPRESENTABLE_YEAR 0
#define DATE_LUT_MAX_REPRESENTABLE_YEAR 9999

#define DATE_LUT_SIZE 0x23AB1

#define DATE_LUT_MAX (0xFFFFFFFFU - 86400)
#define DATE_LUT_MAX_DAY_NUM 0xFFFF

#define DAYNUM_OFFSET_EPOCH 25567

/// Max int value of Date32, DATE LUT cache size minus daynum_offset_epoch
#define DATE_LUT_MAX_EXTEND_DAY_NUM (DATE_LUT_SIZE - DAYNUM_OFFSET_EPOCH)

/// A constant to add to time_t so every supported time point becomes non-negative and still has the same remainder of division by 3600.
/// If we treat "remainder of division" operation in the sense of modular arithmetic (not like in C++).
/// It must cover the whole representable range (down to year 0000), otherwise the relative hour/minute helpers below
/// would feed a negative dividend into C++ truncating division and round pre-1970 values towards zero instead of -inf.
#define DATE_LUT_ADD ((1970 - DATE_LUT_MIN_REPRESENTABLE_YEAR) * 366L * 86400)
/// NOLINTEND(modernize-macro-to-enum)


/// Flags for toYearWeek() function.
enum class WeekModeFlag : UInt8
{
    MONDAY_FIRST = 1,
    YEAR = 2,
    FIRST_WEEKDAY = 4,
    NEWYEAR_DAY = 8
};
using YearWeek = std::pair<UInt16, UInt8>;

/// Modes for toDayOfWeek() function.
enum class WeekDayMode : uint8_t
{
    WeekStartsMonday1 = 0,
    WeekStartsMonday0 = 1,
    WeekStartsSunday0 = 2,
    WeekStartsSunday1 = 3
};

namespace DB
{
class DateTime64;
}

/** Lookup table to conversion of time to date, and to month / year / day of week / day of month and so on.
  * First time was implemented for OLAPServer, that needed to do billions of such transformations.
  */
class DateLUTImpl
{
private:
    friend class DateLUT;
    explicit DateLUTImpl(std::string_view time_zone);

    DateLUTImpl(const DateLUTImpl &) = delete; /// NOLINT
    DateLUTImpl & operator=(const DateLUTImpl &) = delete; /// NOLINT
    DateLUTImpl(const DateLUTImpl &&) = delete; /// NOLINT
    DateLUTImpl & operator=(const DateLUTImpl &&) = delete; /// NOLINT

    // Normalized and bound-checked index of element in lut,
    // has to be a separate type to support overloading
    // TODO: make sure that any arithmetic on LUTIndex actually results in valid LUTIndex.
    STRONG_TYPEDEF(UInt32, LUTIndex)
    // Same as above but select different function overloads for zero saturation.
    STRONG_TYPEDEF(UInt32, LUTIndexWithSaturation)

    static LUTIndex normalizeLUTIndex(UInt32 index)
    {
        if (index >= DATE_LUT_SIZE)
            return LUTIndex(DATE_LUT_SIZE - 1);
        return LUTIndex{index};
    }

    static LUTIndex normalizeLUTIndex(Int64 index)
    {
        if (unlikely(index < 0))
            return LUTIndex(0);
        if (index >= DATE_LUT_SIZE)
            return LUTIndex(DATE_LUT_SIZE - 1);
        return LUTIndex{static_cast<UInt32>(index)};
    }

    template <typename T>
    friend LUTIndex operator+(const LUTIndex & index, const T v)
    {
        return normalizeLUTIndex(index.toUnderType() + UInt32(v));
    }

    template <typename T>
    friend LUTIndex operator+(const T v, const LUTIndex & index)
    {
        return normalizeLUTIndex(static_cast<Int64>(v + index.toUnderType()));
    }

    friend LUTIndex operator+(const LUTIndex & index, const LUTIndex & v)
    {
        return normalizeLUTIndex(static_cast<UInt32>(index.toUnderType() + v.toUnderType()));
    }

    template <typename T>
    friend LUTIndex operator-(const LUTIndex & index, const T v)
    {
        return normalizeLUTIndex(static_cast<Int64>(index.toUnderType() - UInt32(v)));
    }

    template <typename T>
    friend LUTIndex operator-(const T v, const LUTIndex & index)
    {
        return normalizeLUTIndex(static_cast<Int64>(v - index.toUnderType()));
    }

    friend LUTIndex operator-(const LUTIndex & index, const LUTIndex & v)
    {
        return normalizeLUTIndex(static_cast<Int64>(index.toUnderType() - v.toUnderType()));
    }

    template <typename T>
    friend LUTIndex operator*(const LUTIndex & index, const T v)
    {
        return normalizeLUTIndex(index.toUnderType() * UInt32(v));
    }

    template <typename T>
    friend LUTIndex operator*(const T v, const LUTIndex & index)
    {
        return normalizeLUTIndex(v * index.toUnderType());
    }

    template <typename T>
    friend LUTIndex operator/(const LUTIndex & index, const T v)
    {
        return normalizeLUTIndex(index.toUnderType() / UInt32(v));
    }

    template <typename T>
    friend LUTIndex operator/(const T v, const LUTIndex & index)
    {
        return normalizeLUTIndex(UInt32(v) / index.toUnderType());
    }

public:
    /// We use Int64 instead of time_t because time_t is mapped to the different types (long or long long)
    /// on Linux and Darwin (on both of them, long and long long are 64 bit and behaves identically,
    /// but they are different types in C++ and this affects function overload resolution).
    using Time = Int64;

    /// The order of fields matters for alignment and sizeof.
    struct Values
    {
        /// Time at beginning of the day.
        Time date;

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

        Int32 amount_of_offset_change() const /// NOLINT
        {
            return static_cast<Int32>(amount_of_offset_change_value) * OffsetChangeFactor;
        }

        UInt32 time_at_offset_change() const /// NOLINT
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
    /// Offset to epoch in days (ExtendedDayNum) of the first day in LUT.
    /// "epoch" is the Unix Epoch (starts at unix timestamp zero)
    static constexpr UInt32 daynum_offset_epoch = 25567;
    static_assert(daynum_offset_epoch == (1970 - DATE_LUT_MIN_YEAR) * 365 + (1970 - DATE_LUT_MIN_YEAR / 4 * 4) / 4);

    /// Lookup table is indexed by LUTIndex.
    /// Day nums are the same in all time zones. 1970-01-01 is 0 and so on.
    /// Table is relatively large, so better not to place the object on stack.
    /// In comparison to std::vector, plain array is cheaper by one indirection.
    Values lut[DATE_LUT_SIZE + 1]; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - fully assigned in constructor

    /// Same as above but with dates < 1970-01-01 saturated to 1970-01-01.
    Values lut_saturated[DATE_LUT_SIZE + 1]; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - fully assigned in constructor

    /// Year number after DATE_LUT_MIN_YEAR -> LUTIndex in lut for start of year.
    LUTIndex years_lut[DATE_LUT_YEARS];

    /// Year number after DATE_LUT_MIN_YEAR * month number starting at zero -> day num for first day of month
    LUTIndex years_months_lut[DATE_LUT_YEARS * 12];

    /// UTC offset at beginning of the Unix epoch. The same as unix timestamp of 1970-01-01 00:00:00 local time.
    Time offset_at_start_of_epoch;
    /// UTC offset at the beginning of the first supported year.
    Time offset_at_start_of_lut;
    bool offset_is_whole_number_of_hours_during_epoch;
    bool offset_is_whole_number_of_minutes_during_epoch;

    /// Time zone name.
    std::string time_zone;

    /// The loaded cctz time zone, kept for the out-of-range escape paths below.
    /// The lookup table only covers [DATE_LUT_MIN_YEAR, DATE_LUT_MAX_YEAR]; for time points and day numbers
    /// outside of this range we fall back to cctz directly instead of clamping to the boundary.
    std::unique_ptr<cctz::time_zone> cctz_time_zone;

    /// The lookup table covers the local days [DATE_LUT_MIN_YEAR-01-01, DATE_LUT_MAX_YEAR-12-31]. The exact
    /// Unix-time boundary of that range is time-zone dependent, but it cannot shift by more than the largest
    /// possible UTC offset (well under a day). So a window shrunk by two days on each side from the UTC bounds
    /// is guaranteed to be in range for *every* time zone, and uses only compile-time constants — no per-instance
    /// members and no loads on the hot path. The few near-boundary time points that fall outside this window take
    /// the (always-correct) cctz escape path. Day numbers are time-zone independent, so they need no margin.
    static constexpr Time lut_in_range_min = -2208988800 + 2 * 86400; /// after 1900-01-01 00:00:00 UTC, for any time zone
    static constexpr Time lut_in_range_max = 10413792000 - 2 * 86400; /// before 2300-01-01 00:00:00 UTC, for any time zone

    /// The widest range DateLUTImpl can represent and format: a 4-digit year, i.e. [0000-01-01, 9999-12-31].
    /// The cctz escape paths clamp to this window so that formatting can never overflow the 4-digit year
    /// (values further out of range are produced e.g. by fromUnixTimestamp64 with a huge argument).
    static constexpr Int64 min_representable_day_index = -693961;  /// 0000-01-01, days since DATE_LUT_MIN_YEAR-01-01
    static constexpr Int64 max_representable_day_index = 2958463;  /// 9999-12-31
    static constexpr Time min_representable_time = -62167219200;   /// 0000-01-01 00:00:00 UTC
    static constexpr Time max_representable_time = 253402300799;   /// 9999-12-31 23:59:59 UTC

    /// std::chrono::system_clock::from_time_t can overflow for extreme Int64 inputs, so the cctz escape paths
    /// bound the UTC timestamp to this window before constructing a time point. It is wider than the
    /// representable calendar by more than a whole-day timezone offset, so that a local civil value at the
    /// [0000, 9999] boundary (whose UTC instant can lie just outside the representable window) survives the
    /// conversion; the local result is clamped to the representable calendar after the timezone conversion.
    static constexpr Time min_chrono_safe_time = min_representable_time - 2 * 86400;
    static constexpr Time max_chrono_safe_time = max_representable_time + 2 * 86400;

    /// Whether a value cannot be represented by the lookup table and needs the cctz escape path.
    /// Only Time (timestamp) and ExtendedDayNum (signed day number) can fall outside of the LUT;
    /// DateTime (UInt32), Date (DayNum/UInt16) and LUTIndex always fit, so they never take the escape path.
    template <typename T>
    static constexpr bool may_be_out_of_lut_range = std::is_same_v<T, Time> || std::is_same_v<T, ExtendedDayNum>;

    static bool isOutOfLUTRange(Time t)
    {
        /// Single unsigned comparison against compile-time constants, equivalent to
        /// (t < lut_in_range_min || t >= lut_in_range_max). Cast before subtracting to avoid signed overflow.
        return static_cast<UInt64>(t) - static_cast<UInt64>(lut_in_range_min) >= static_cast<UInt64>(lut_in_range_max - lut_in_range_min);
    }

    static bool isOutOfLUTRange(ExtendedDayNum d)
    {
        /// Single unsigned comparison, equivalent to (index < 0 || index >= DATE_LUT_SIZE).
        return static_cast<UInt32>(static_cast<Int64>(d.toUnderType()) + daynum_offset_epoch) >= DATE_LUT_SIZE;
    }

    /// The day index (number of days since DATE_LUT_MIN_YEAR-01-01, i.e. a non-normalized LUTIndex value)
    /// of an out-of-range value. For day numbers it is plain arithmetic; for time points it needs cctz.
    Int64 outOfRangeDayIndex(Time t) const { return findDayIndexOutOfRange(t); }
    /// Saturate to the representable window so callers stay consistent with valuesForOutOfRangeDayIndex (which also
    /// clamps); an extreme Date32 day number would otherwise yield a nonsensical day-of-year / start-of-week / etc.
    static Int64 outOfRangeDayIndex(ExtendedDayNum d)
    {
        return std::clamp(static_cast<Int64>(d.toUnderType()) + daynum_offset_epoch, min_representable_day_index, max_representable_day_index);
    }

    /// The Values of the day an out-of-range value belongs to.
    Values outOfRangeValues(Time t) const { return valuesForOutOfRangeDayIndex(findDayIndexOutOfRange(t)); }
    Values outOfRangeValues(ExtendedDayNum d) const { return valuesForOutOfRangeDayIndex(outOfRangeDayIndex(d)); }

    /// Day number (ExtendedDayNum, counted from the Unix epoch) corresponding to a day index (counted from DATE_LUT_MIN_YEAR).
    static ExtendedDayNum dayNumOfDayIndex(Int64 day_index)
    {
        return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(day_index - daynum_offset_epoch)};
    }
    /// Start-of-day Time of a day index, with the cctz escape path for out-of-range indices.
    Time dateOfDayIndex(Int64 day_index) const { return valuesForOutOfRangeDayIndex(day_index).date; }

    /// The Gregorian (and ISO-week) calendar repeats exactly every 400 years == DATE_LUT_SIZE days, and week
    /// numbering is timezone-independent. So for out-of-range day numbers we shift into the LUT range by whole
    /// 400-year cycles, reuse the in-range logic, and adjust the resulting year by 400 * (number of cycles).
    /// Returns the in-range day number; `cycles` receives the number of cycles added (negative if subtracted).
    static constexpr Int64 days_in_400_years = DATE_LUT_SIZE;
    ExtendedDayNum shiftIntoLUTRange(ExtendedDayNum d, Int32 & cycles) const
    {
        Int64 index = static_cast<Int64>(d.toUnderType()) + daynum_offset_epoch;
        Int64 k = 0;
        if (index < 0)
            k = (days_in_400_years - 1 - index) / days_in_400_years;
        else if (index >= days_in_400_years)
            k = -(index / days_in_400_years);
        index += k * days_in_400_years;
        cycles = static_cast<Int32>(k);
        return dayNumOfDayIndex(index);
    }

    /// The escape paths implemented with cctz (findDayIndexOutOfRange, valuesForOutOfRangeDayIndex,
    /// toDateTimeComponentsOutOfRange, addDaysOutOfRange, makeDateOutOfRange, ...) are declared after the
    /// DateTimeComponents struct below, since some of them return it. They are defined in DateLUTImpl.cpp.

    LUTIndex findIndex(Time t) const
    {
        /// First guess.
        Time guess = (t / 86400) + daynum_offset_epoch;

        /// For negative Time the integer division was rounded up, so the guess is offset by one.
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
                return LUTIndex(static_cast<unsigned>(guess));

            return LUTIndex(static_cast<unsigned>(guess) + 1);
        }

        return LUTIndex(guess ? static_cast<unsigned>(guess) - 1 : 0);
    }

    /// Same as findIndex, but for a Time that the caller has already established to be in range (i.e. that passed
    /// the `!isOutOfLUTRange(t)` gate). The gate's two-day margin guarantees the guess is in [1, DATE_LUT_SIZE - 2],
    /// so the bound clamps that findIndex repeats are unnecessary and lut[guess - 1 .. guess + 1] are all valid.
    LUTIndex findIndexInRange(Time t) const
    {
        Time guess = (t / 86400) + daynum_offset_epoch;
        if (unlikely(t < 0))
            --guess;

        if (t >= lut[guess].date)
        {
            if (t < lut[guess + 1].date)
                return LUTIndex(static_cast<unsigned>(guess));
            return LUTIndex(static_cast<unsigned>(guess) + 1);
        }
        return LUTIndex(static_cast<unsigned>(guess) - 1);
    }

    static LUTIndex toLUTIndex(DayNum d)
    {
        return normalizeLUTIndex(d + daynum_offset_epoch);
    }

    static LUTIndex toLUTIndex(ExtendedDayNum d)
    {
        return normalizeLUTIndex(static_cast<Int64>(d) + daynum_offset_epoch);
    }

    LUTIndex toLUTIndex(Time t) const
    {
        return findIndex(t);
    }

    static LUTIndex toLUTIndex(LUTIndex i)
    {
        return i;
    }

    template <typename DateOrTime>
    const Values & find(DateOrTime v) const
    {
        return lut[toLUTIndex(v)];
    }

    /// Round `value` down to a multiple of `divisor` (towards negative infinity).
    /// Integer division truncates towards zero, so for negative values we shift the result one step down.
    /// The computation goes through the remainder to avoid signed overflow when `value` is close to the
    /// minimum of the type - the natural expression `value + 1 - divisor` overflows there. Such values are
    /// far outside any valid date range, so on the boundary we saturate to the nearest representable multiple.
    template <typename DateOrTime, typename Divisor>
    static DateOrTime roundDownToMultiple(DateOrTime value, Divisor divisor)
    {
        if (value >= 0) [[likely]]
            return static_cast<DateOrTime>(value / divisor * divisor);

        const Int64 v = static_cast<Int64>(value);
        const Int64 d = static_cast<Int64>(divisor);
        const Int64 remainder = v % d; /// In (-d, 0] for negative v.
        if (remainder == 0)
            return static_cast<DateOrTime>(v);

        const Int64 rounded_towards_zero = v - remainder; /// A multiple of d in [v, 0], never overflows.
        if (unlikely(rounded_towards_zero < std::numeric_limits<Int64>::min() + d))
            return static_cast<DateOrTime>(rounded_towards_zero);
        return static_cast<DateOrTime>(rounded_towards_zero - d);
    }

    /// Add `offset` to `base`, saturating at the boundaries of `Time` instead of overflowing (which is
    /// undefined behavior). Interval rounding reconstructs the result as `date + offset`; for arguments far
    /// outside any valid date range this sum can step just past the type boundary even though both operands
    /// are individually representable. The result for such out-of-range values is meaningless anyway, so we
    /// saturate to the nearest boundary.
    static Time addSaturating(Time base, Time offset)
    {
        Time res = 0;
        if (unlikely(__builtin_add_overflow(base, offset, &res)))
            return offset < 0 ? std::numeric_limits<Time>::min() : std::numeric_limits<Time>::max();
        return res;
    }

    template <typename DateOrTime, typename Divisor>
    DateOrTime roundDown(DateOrTime x, Divisor divisor) const
    {
        static_assert(std::is_integral_v<DateOrTime> && std::is_integral_v<Divisor>);
        chassert(divisor > 0);

        /// Checked before the fast path below: the "whole number of hours" property holds during the epoch,
        /// but historical (pre-1900) offsets can have a sub-hour component (e.g. Moscow's +2:30:17 LMT), so the
        /// fast path would round to a UTC boundary instead of the local one for out-of-range values.
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(x)))
            {
                const Time date = outOfRangeValues(x).date;
                return static_cast<DateOrTime>(date + (static_cast<Time>(x) - date) / divisor * divisor);
            }

        if (offset_is_whole_number_of_hours_during_epoch) [[likely]]
            return roundDownToMultiple(x, divisor);

        const Time date = find(x).date;
        Time res = date + (x - date) / divisor * divisor;
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
        {
            if (unlikely(res < 0))
                return 0;
            return static_cast<DateOrTime>(res);
        }
        else
            return res;
    }

public:
    /// Defined in the .cpp because of the forward-declared cctz::time_zone held in a unique_ptr.
    ~DateLUTImpl();

    const std::string & getTimeZone() const { return time_zone; }

    // Methods only for unit-testing, it makes very little sense to use it from user code.
    auto getOffsetAtStartOfEpoch() const { return offset_at_start_of_epoch; }
    auto getTimeOffsetAtStartOfLUT() const { return offset_at_start_of_lut; }

    static constexpr auto getDayNumOffsetEpoch()  { return daynum_offset_epoch; }

    /// All functions below are thread-safe; arguments are not checked.

    static UInt32 saturateMinus(UInt32 x, UInt32 y)
    {
        UInt32 res = x - y;
        res &= -Int32(res <= x);
        return res;
    }

    static ExtendedDayNum toDayNum(ExtendedDayNum d)
    {
        return d;
    }

    static ExtendedDayNum toDayNum(LUTIndex d)
    {
        return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(d.toUnderType() - daynum_offset_epoch)};
    }

    static DayNum toDayNum(LUTIndexWithSaturation d)
    {
        return DayNum{static_cast<DayNum::UnderlyingType>(saturateMinus(d.toUnderType(), daynum_offset_epoch))};
    }

    template <typename DateOrTime>
    auto toDayNum(DateOrTime v) const
    {
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return DayNum{static_cast<DayNum::UnderlyingType>(saturateMinus(toLUTIndex(v).toUnderType(), daynum_offset_epoch))};
        else
        {
            if constexpr (may_be_out_of_lut_range<DateOrTime>)
                if (unlikely(isOutOfLUTRange(v)))
                    return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(outOfRangeDayIndex(v) - daynum_offset_epoch)};
            return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(toLUTIndex(v).toUnderType() - daynum_offset_epoch)};
        }
    }

    /// Round down to start of monday.
    template <typename DateOrTime>
    Time toFirstDayOfWeek(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dateOfDayIndex(outOfRangeDayIndex(v) - (outOfRangeValues(v).day_of_week - 1));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return lut_saturated[i - (lut[i].day_of_week - 1)].date;
        else
            return lut[i - (lut[i].day_of_week - 1)].date;
    }

    template <typename DateOrTime>
    auto toFirstDayNumOfWeek(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dayNumOfDayIndex(outOfRangeDayIndex(v) - (outOfRangeValues(v).day_of_week - 1));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(i - (lut[i].day_of_week - 1)));
        else
            return toDayNum(LUTIndex(i - (lut[i].day_of_week - 1)));
    }

    /// Round up to the last day of week.
    template <typename DateOrTime>
    Time toLastDayOfWeek(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dateOfDayIndex(outOfRangeDayIndex(v) + (7 - outOfRangeValues(v).day_of_week));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return lut_saturated[i + (7 - lut[i].day_of_week)].date;
        else
            return lut[i + (7 - lut[i].day_of_week)].date;
    }

    template <typename DateOrTime>
    auto toLastDayNumOfWeek(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dayNumOfDayIndex(outOfRangeDayIndex(v) + (7 - outOfRangeValues(v).day_of_week));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(i + (7 - lut[i].day_of_week)));
        else
            return toDayNum(LUTIndex(i + (7 - lut[i].day_of_week)));
    }

    /// Round down to start of month.
    template <typename DateOrTime>
    Time toFirstDayOfMonth(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dateOfDayIndex(outOfRangeDayIndex(v) - (outOfRangeValues(v).day_of_month - 1));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return lut_saturated[i - (lut[i].day_of_month - 1)].date;
        else
            return lut[i - (lut[i].day_of_month - 1)].date;
    }

    template <typename DateOrTime>
    auto toFirstDayNumOfMonth(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return dayNumOfDayIndex(outOfRangeDayIndex(v) - (outOfRangeValues(v).day_of_month - 1));

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(i - (lut[i].day_of_month - 1)));
        else
            return toDayNum(LUTIndex(i - (lut[i].day_of_month - 1)));
    }

    /// Round up to last day of month.
    template <typename DateOrTime>
    Time toLastDayOfMonth(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Values values = outOfRangeValues(v);
                return dateOfDayIndex(outOfRangeDayIndex(v) + (values.days_in_month - values.day_of_month));
            }

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return lut_saturated[i + (lut[i].days_in_month - lut[i].day_of_month)].date;
        else
            return lut[i + (lut[i].days_in_month - lut[i].day_of_month)].date;
    }

    template <typename DateOrTime>
    auto toLastDayNumOfMonth(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Values values = outOfRangeValues(v);
                return dayNumOfDayIndex(outOfRangeDayIndex(v) + (values.days_in_month - values.day_of_month));
            }

        const LUTIndex i = toLUTIndex(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(i + (lut[i].days_in_month - lut[i].day_of_month)));
        else
            return toDayNum(LUTIndex(i + (lut[i].days_in_month - lut[i].day_of_month)));
    }

    /// Round down to start of quarter.
    template <typename DateOrTime>
    auto toFirstDayNumOfQuarter(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Values values = outOfRangeValues(v);
                return makeDayNumOutOfRange(values.year, (values.month - 1) / 3 * 3 + 1, 1);
            }

        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(toFirstDayOfQuarterIndex(v)));
        else
            return toDayNum(LUTIndex(toFirstDayOfQuarterIndex(v)));
    }

    template <typename DateOrTime>
    LUTIndex toFirstDayOfQuarterIndex(DateOrTime v) const
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
    Time toFirstDayOfQuarter(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Values values = outOfRangeValues(v);
                return makeDateOutOfRange(values.year, (values.month - 1) / 3 * 3 + 1, 1);
            }

        return toDate(toFirstDayOfQuarterIndex(v));
    }

    /// Round down to start of year.
    Time toFirstDayOfYear(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return makeDateOutOfRange(outOfRangeValues(t).year, 1, 1);

        return lut[years_lut[lut[findIndexInRange(t)].year - DATE_LUT_MIN_YEAR]].date;
    }

    template <typename DateOrTime>
    LUTIndex toFirstDayNumOfYearIndex(DateOrTime v) const
    {
        return years_lut[lut[toLUTIndex(v)].year - DATE_LUT_MIN_YEAR];
    }

    template <typename DateOrTime>
    auto toFirstDayNumOfYear(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return makeDayNumOutOfRange(outOfRangeValues(v).year, 1, 1);

        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(toFirstDayNumOfYearIndex(v)));
        else
            return toDayNum(LUTIndex(toFirstDayNumOfYearIndex(v)));
    }

    Time toFirstDayOfNextMonth(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
        {
            const Values values = outOfRangeValues(t);
            return makeDateOutOfRange(values.year, values.month + 1, 1);
        }

        LUTIndex index = findIndexInRange(t);
        index += 32 - lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    Time toFirstDayOfPrevMonth(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
        {
            const Values values = outOfRangeValues(t);
            return makeDateOutOfRange(values.year, values.month - 1, 1);
        }

        LUTIndex index = findIndexInRange(t);
        index -= lut[index].day_of_month;
        return lut[index - (lut[index].day_of_month - 1)].date;
    }

    template <typename DateOrTime>
    UInt8 daysInMonth(DateOrTime value) const
    {
        return getValues(value).days_in_month;
    }

    UInt8 daysInMonth(Int16 year, UInt8 month) const
    {
        UInt16 idx = year - DATE_LUT_MIN_YEAR;
        if (likely(idx < DATE_LUT_YEARS))
        {
            /// 32 makes arithmetic more simple.
            const auto any_day_of_month = years_lut[year - DATE_LUT_MIN_YEAR] + 32 * (month - 1);
            return lut[any_day_of_month].days_in_month;
        }

        /// Out-of-range year: compute with cctz via the first day of the month.
        const Int64 first_of_month = static_cast<Int64>(makeDayNumOutOfRange(year, month, 1)) + daynum_offset_epoch;
        return valuesForOutOfRangeDayIndex(first_of_month).days_in_month;
    }

    /** Round to start of day, then shift for specified amount of days.
      */
    Time toDateAndShift(Time t, Int32 days) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return dateOfDayIndex(findDayIndexOutOfRange(t) + days);

        const LUTIndex index = findIndexInRange(t);
        const Int64 shifted = static_cast<Int64>(index.toUnderType()) + days;
        if (unlikely(shifted < 0 || shifted >= DATE_LUT_SIZE))
            return dateOfDayIndex(shifted);

        return lut[index + days].date;
    }

    Time toTime(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return toTimeOutOfRange(t);

        const LUTIndex index = findIndexInRange(t);

        Time res = t - lut[index].date;

        if (res >= lut[index].time_at_offset_change())
            res += lut[index].amount_of_offset_change();

        return res - offset_at_start_of_epoch; /// Starting at 1970-01-01 00:00:00 local time.
    }

    unsigned toHour(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return static_cast<unsigned>(toDateTimeComponentsOutOfRange(t).time.hour);

        const LUTIndex index = findIndexInRange(t);

        Time time = t - lut[index].date;

        if (time >= lut[index].time_at_offset_change())
            time += lut[index].amount_of_offset_change();

        unsigned res = static_cast<unsigned>(time / 3600);

        /// In case time was changed backwards at the start of next day, we will repeat the hour 23.
        return res <= 23 ? res : 23;
    }

    /** Calculating offset from UTC in seconds.
      * which means Using the same literal time of "t" to get the corresponding timestamp in UTC,
      * then subtract the former from the latter to get the offset result.
      * The boundaries when meets DST(daylight saving time) change should be handled very carefully.
      */
    Time timezoneOffset(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return timezoneOffsetOutOfRange(t);

        const LUTIndex index = findIndexInRange(t);

        /// Calculate daylight saving offset first.
        /// Because the "amount_of_offset_change" in LUT entry only exists in the change day, it's costly to scan it from the very begin.
        /// but we can figure out all the accumulated offsets from 1970-01-01 to that day just by get the whole difference between lut[].date,
        /// and then, we can directly subtract multiple 86400s to get the real DST offsets for the leap seconds is not considered now.
        Time res = (lut[index].date - lut[daynum_offset_epoch].date) % 86400;

        /// As so far to know, the maximal DST offset couldn't be more than 2 hours, so after the modulo operation the remainder
        /// will sits between [-offset --> 0 --> offset] which respectively corresponds to moving clock forward or backward.
        res = res > 43200 ? (86400 - res) : (0 - res);

        /// Check if has a offset change during this day. Add the change when cross the line
        if (lut[index].amount_of_offset_change() != 0 && t >= lut[index].date + lut[index].time_at_offset_change())
            res += lut[index].amount_of_offset_change();

        return res + offset_at_start_of_epoch;
    }


    unsigned toSecond(Time t) const
    {
        /// Checked before the fast path: the "whole number of minutes" property holds during the epoch,
        /// but historical (pre-1900) offsets can have a sub-minute component (e.g. Moscow's +2:30:17 LMT).
        if (unlikely(isOutOfLUTRange(t)))
            return static_cast<unsigned>(toDateTimeComponentsOutOfRange(t).time.second);

        if (offset_is_whole_number_of_minutes_during_epoch) [[likely]]
        {
            Time res = t % 60;
            if (res >= 0) [[likely]]
                return static_cast<unsigned>(res);
            return static_cast<unsigned>(res) + 60;
        }

        LUTIndex index = findIndexInRange(t);
        Time time = t - lut[index].date;

        if (time >= lut[index].time_at_offset_change())
            time += lut[index].amount_of_offset_change();

        return time % 60;
    }

    unsigned toMillisecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const;

    unsigned toMicrosecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const;

    unsigned toNanosecond(const DB::DateTime64 & datetime, Int64 scale_multiplier) const;

    unsigned toMinute(Time t) const
    {
        /// Checked before the fast path: historical (pre-1900) offsets can have a sub-minute component.
        if (unlikely(isOutOfLUTRange(t)))
            return static_cast<unsigned>(toDateTimeComponentsOutOfRange(t).time.minute);

        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return (t / 60) % 60;

        /// To consider the DST changing situation within this day
        /// also make the special timezones with no whole hour offset such as 'Australia/Lord_Howe' been taken into account.

        LUTIndex index = findIndexInRange(t);
        UInt32 time = static_cast<UInt32>(t - lut[index].date);

        if (time >= lut[index].time_at_offset_change())
            time += lut[index].amount_of_offset_change();

        return time / 60 % 60;
    }

    /// NOTE: Assuming timezone offset is a multiple of 15 minutes.
    template <typename DateOrTime>
    DateOrTime toStartOfMinute(DateOrTime t) const { return toStartOfMinuteInterval(t, 1); }
    template <typename DateOrTime>
    DateOrTime toStartOfFiveMinutes(DateOrTime t) const { return toStartOfMinuteInterval(t, 5); }
    template <typename DateOrTime>
    DateOrTime toStartOfFifteenMinutes(DateOrTime t) const { return toStartOfMinuteInterval(t, 15); }
    template <typename DateOrTime>
    DateOrTime toStartOfTenMinutes(DateOrTime t) const { return toStartOfMinuteInterval(t, 10); }
    template <typename DateOrTime>
    DateOrTime toStartOfHour(DateOrTime t) const { return roundDown(t, 3600); }

    /** Number of calendar day since the beginning of UNIX epoch (1970-01-01 is zero)
      * We use just two bytes for it. It covers the range up to 2105 and slightly more.
      *
      * This is "calendar" day, it itself is independent of time zone
      * (conversion from/to unix timestamp will depend on time zone,
      *  because the same calendar day starts/ends at different timestamps in different time zones)
      */

    Time fromDayNum(DayNum d) const { return lut_saturated[toLUTIndex(d)].date; }
    Time fromDayNum(ExtendedDayNum d) const
    {
        if (unlikely(isOutOfLUTRange(d)))
            return outOfRangeValues(d).date;
        return lut[toLUTIndex(d)].date;
    }

    template <typename DateOrTime>
    Time toDate(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
                return outOfRangeValues(v).date;

        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return lut_saturated[toLUTIndex(v)].date;
        else
            return lut[toLUTIndex(v)].date;
    }

    template <typename DateOrTime>
    UInt8 toMonth(DateOrTime v) const { return getValues(v).month; }

    template <typename DateOrTime>
    UInt8 toQuarter(DateOrTime v) const { return (getValues(v).month - 1) / 3 + 1; }

    template <typename DateOrTime>
    Int16 toYear(DateOrTime v) const { return getValues(v).year; }

    template <typename DateOrTime>
    Int16 toYearSinceEpoch(DateOrTime v) const { return getValues(v).year - 1970; }

    /// 1-based, starts on Monday
    template <typename DateOrTime>
    UInt8 toDayOfWeek(DateOrTime v) const { return getValues(v).day_of_week; }

    template <typename DateOrTime>
    UInt8 toDayOfWeek(DateOrTime v, UInt8 week_day_mode) const
    {
        WeekDayMode mode = check_week_day_mode(week_day_mode);

        UInt8 res = toDayOfWeek(v);
        using enum WeekDayMode;
        bool start_from_sunday = (mode == WeekStartsSunday0 || mode == WeekStartsSunday1);
        bool zero_based = (mode == WeekStartsMonday0 || mode == WeekStartsSunday0);

        if (start_from_sunday)
            res = res % 7 + 1;
        if (zero_based)
            --res;

        return res;
    }

    template <typename DateOrTime>
    UInt8 toDayOfMonth(DateOrTime v) const { return getValues(v).day_of_month; }

    template <typename DateOrTime>
    UInt16 toDayOfYear(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Values values = outOfRangeValues(v);
                const Int64 day_index = outOfRangeDayIndex(v);
                const Int64 first_day_of_year_index = static_cast<Int64>(makeDayNumOutOfRange(values.year, 1, 1)) + daynum_offset_epoch;
                return static_cast<UInt16>(day_index - first_day_of_year_index + 1);
            }

        // TODO: different overload for ExtendedDayNum
        const LUTIndex i = toLUTIndex(v);
        return static_cast<UInt16>(i + 1 - toFirstDayNumOfYearIndex(i));
    }

    /// Number of week from some fixed moment in the past. Week begins at monday.
    /// (round down to monday and divide DayNum by 7; we made an assumption,
    ///  that in domain of the function there was no weeks with any other number of days than 7)
    template <typename DateOrTime>
    Int32 toRelativeWeekNum(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                /// Mirror the in-range formula in day-number space (toDayNum(i + (8 - dow)) / 7), using floor
                /// division so a pre-epoch week number rounds towards -inf; otherwise dateDiff('week', ...) undercounts.
                const Int64 day_index = outOfRangeDayIndex(v);
                const UInt8 day_of_week = outOfRangeValues(v).day_of_week;
                const Int64 shifted = day_index + (8 - day_of_week) - daynum_offset_epoch;
                return static_cast<Int32>(shifted >= 0 ? shifted / 7 : -((-shifted + 6) / 7));
            }

        const LUTIndex i = toLUTIndex(v);
        /// We add 8 to avoid underflow at beginning of unix epoch.
        return toDayNum(i + (8 - toDayOfWeek(i))) / 7;
    }

    /// Get year that contains most of the current week. Week begins at monday.
    template <typename DateOrTime>
    Int16 toISOYear(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                Int32 cycles = 0;
                const ExtendedDayNum shifted = shiftIntoLUTRange(toDayNum(v), cycles);
                /// The ISO year can fall just outside [0000, 9999] at the boundaries (e.g. 0000-01-01 belongs
                /// to ISO year -1); clamp it so the returned year stays within the representable range.
                Int32 iso_year = toISOYear(shifted) - cycles * 400;
                if (iso_year < DATE_LUT_MIN_REPRESENTABLE_YEAR)
                    iso_year = DATE_LUT_MIN_REPRESENTABLE_YEAR;
                else if (iso_year > DATE_LUT_MAX_REPRESENTABLE_YEAR)
                    iso_year = DATE_LUT_MAX_REPRESENTABLE_YEAR;
                return static_cast<Int16>(iso_year);
            }

        const LUTIndex i = toLUTIndex(v);
        /// That's effectively the year of thursday of current week.
        return toYear(toLUTIndex(i + (4 - toDayOfWeek(i))));
    }

    /// ISO year begins with a monday of the week that is contained more than by half in the corresponding calendar year.
    /// Example: ISO year 2019 begins at 2018-12-31. And ISO year 2017 begins at 2017-01-02.
    /// https://en.wikipedia.org/wiki/ISO_week_date
    template <typename DateOrTime>
    LUTIndex toFirstDayNumOfISOYearIndex(DateOrTime v) const
    {
        const LUTIndex i = toLUTIndex(v);
        auto iso_year = toISOYear(i);

        const auto first_day_of_year = years_lut[iso_year - DATE_LUT_MIN_YEAR];
        auto first_day_of_week_of_year = lut[first_day_of_year].day_of_week;

        return LUTIndex{first_day_of_week_of_year <= 4
            ? first_day_of_year + (1 - first_day_of_week_of_year)
            : first_day_of_year + (8 - first_day_of_week_of_year)};
    }

    template <typename DateOrTime>
    auto toFirstDayNumOfISOYear(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                Int32 cycles = 0;
                const ExtendedDayNum shifted = shiftIntoLUTRange(toDayNum(v), cycles);
                /// Not recursing into toFirstDayNumOfISOYear (it has a deduced return type); use the index helper directly.
                const ExtendedDayNum first = toDayNum(toFirstDayNumOfISOYearIndex(shifted));
                return ExtendedDayNum{static_cast<ExtendedDayNum::UnderlyingType>(first.toUnderType() - cycles * days_in_400_years)};
            }

        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(toFirstDayNumOfISOYearIndex(v)));
        else
            return toDayNum(LUTIndex(toFirstDayNumOfISOYearIndex(v)));
    }

    Time toFirstDayOfISOYear(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
        {
            const ExtendedDayNum first = toFirstDayNumOfISOYear(t);
            return dateOfDayIndex(static_cast<Int64>(first.toUnderType()) + daynum_offset_epoch);
        }

        return lut[toFirstDayNumOfISOYearIndex(t)].date;
    }

    /// ISO 8601 week number. Week begins at monday.
    /// The week number 1 is the first week in year that contains 4 or more days (that's more than half).
    template <typename DateOrTime>
    UInt8 toISOWeek(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                /// The ISO week number is timezone-independent and repeats every 400 years.
                Int32 cycles = 0;
                return toISOWeek(shiftIntoLUTRange(toDayNum(v), cycles));
            }

        return static_cast<UInt8>(1 + (toFirstDayNumOfWeek(v) - toDayNum(toFirstDayNumOfISOYearIndex(v))) / 7);
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
    YearWeek toYearWeek(DateOrTime v, UInt8 week_mode) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                /// Year/week numbering is timezone-independent and repeats every 400 years.
                Int32 cycles = 0;
                const ExtendedDayNum shifted = shiftIntoLUTRange(toDayNum(v), cycles);
                YearWeek yw = toYearWeek(shifted, week_mode);
                /// The ISO week-year can fall just outside the representable [0000, 9999] range at the
                /// boundaries (e.g. 0000-01-01 is a Saturday belonging to week-year -1, and 9999-12-31 can
                /// belong to week-year 10000); clamp it so the UInt16 YYYYWW result does not wrap around.
                Int32 adjusted_year = static_cast<Int32>(yw.first) - cycles * 400;
                if (adjusted_year < DATE_LUT_MIN_REPRESENTABLE_YEAR)
                    adjusted_year = DATE_LUT_MIN_REPRESENTABLE_YEAR;
                else if (adjusted_year > DATE_LUT_MAX_REPRESENTABLE_YEAR)
                    adjusted_year = DATE_LUT_MAX_REPRESENTABLE_YEAR;
                yw.first = static_cast<UInt16>(adjusted_year);
                return yw;
            }

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
        const auto day_number = makeDayNum(yw.first, toMonth(i), toDayOfMonth(i));
        auto first_day_number = makeDayNum(yw.first, 1, 1);

        // 0 for monday, 1 for tuesday ...
        // get weekday from first day in year.
        UInt8 weekday = calc_weekday(first_day_number, !monday_first_mode);

        if (toMonth(i) == 1 && toDayOfMonth(i) <= static_cast<UInt32>(7 - weekday))
        {
            if (!week_year_mode && ((first_weekday_mode && weekday != 0) || (!first_weekday_mode && weekday >= 4)))
                return yw;
            week_year_mode = true;
            --yw.first;
            days = calc_days_in_year(yw.first);
            first_day_number -= days;
            weekday = (weekday + 53 * 7 - days) % 7;
        }

        if ((first_weekday_mode && weekday != 0) || (!first_weekday_mode && weekday >= 4))
            days = static_cast<UInt16>(day_number - (first_day_number + (7 - weekday)));
        else
            days = static_cast<UInt16>(day_number - (first_day_number - weekday));

        if (week_year_mode && days >= 52 * 7)
        {
            weekday = (weekday + calc_days_in_year(yw.first)) % 7;
            if ((!first_weekday_mode && weekday < 4) || (first_weekday_mode && weekday == 0))
            {
                ++yw.first;
                yw.second = 1;
                return yw;
            }
        }

        yw.second = static_cast<UInt8>(days / 7 + 1);
        return yw;
    }

    /// Calculate week number of WeekModeFlag::NEWYEAR_DAY mode
    /// The week number 1 is the first week in year that contains January 1,
    template <typename DateOrTime>
    YearWeek toYearWeekOfNewyearMode(DateOrTime v, bool monday_first_mode) const
    {
        YearWeek yw(0, 0);
        UInt16 offset_day = monday_first_mode ? 0U : 1U;

        const LUTIndex i = LUTIndex(v);

        // Checking the week across the year
        yw.first = toYear(i + (7 - toDayOfWeek(i + offset_day)));

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
        yw.second = static_cast<UInt8>((this_day - first_day) / 7 + 1);
        return yw;
    }

    /// Get first day of week with week_mode, return Sunday or Monday
    template <typename DateOrTime>
    auto toFirstDayNumOfWeek(DateOrTime v, UInt8 week_mode) const
    {
        bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        if (monday_first_mode)
        {
            return toFirstDayNumOfWeek(v);
        }

        const auto day_of_week = toDayOfWeek(v);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return (day_of_week != 7) ? DayNum(static_cast<UInt16>(saturateMinus(v, day_of_week))) : toDayNum(v);
        else
            return (day_of_week != 7) ? ExtendedDayNum(v - day_of_week) : toDayNum(v);
    }

    /// Get last day of week with week_mode, return Saturday or Sunday
    template <typename DateOrTime>
    auto toLastDayNumOfWeek(DateOrTime v, UInt8 week_mode) const
    {
        bool monday_first_mode = week_mode & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST);
        if (monday_first_mode)
        {
            return toLastDayNumOfWeek(v);
        }

        const auto day_of_week = toDayOfWeek(v);
        v += 6;
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return (day_of_week != 7) ? DayNum(static_cast<UInt16>(saturateMinus(v, day_of_week))) : toDayNum(v);
        else
            return (day_of_week != 7) ? ExtendedDayNum(v - day_of_week) : toDayNum(v);
    }

    /// Check and change mode to effective.
    UInt8 check_week_mode(UInt8 mode) const /// NOLINT
    {
        UInt8 week_format = (mode & 7);
        if (!(week_format & static_cast<UInt8>(WeekModeFlag::MONDAY_FIRST)))
            week_format ^= static_cast<UInt8>(WeekModeFlag::FIRST_WEEKDAY);
        return week_format;
    }

    /// Check and change mode to effective.
    WeekDayMode check_week_day_mode(UInt8 mode) const /// NOLINT
    {
        return static_cast<WeekDayMode>(mode & 3);
    }

    /** Calculate weekday from d.
      * Returns 0 for monday, 1 for tuesday...
      */
    template <typename DateOrTime>
    UInt8 calc_weekday(DateOrTime v, bool sunday_first_day_of_week) const /// NOLINT
    {
        const LUTIndex i = toLUTIndex(v);
        if (!sunday_first_day_of_week)
            return toDayOfWeek(i) - 1;
        return toDayOfWeek(i + 1) - 1;
    }

    /// Calculate days in one year.
    UInt16 calc_days_in_year(Int32 year) const /// NOLINT
    {
        return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? 366 : 365);
    }

    /// Number of month from some fixed moment in the past (year * 12 + month)
    template <typename DateOrTime>
    Int32 toRelativeMonthNum(DateOrTime v) const
    {
        const Values values = getValues(v);
        return values.year * 12 + values.month;
    }

    template <typename DateOrTime>
    Int32 toMonthNumSinceEpoch(DateOrTime v) const
    {
        const Values values = getValues(v);
        return (values.year - 1970) * 12 + values.month - 1;
    }

    template <typename DateOrTime>
    Int32 toRelativeQuarterNum(DateOrTime v) const
    {
        const Values values = getValues(v);
        return values.year * 4 + (values.month - 1) / 3;
    }

    /// We count all hour-length intervals, unrelated to offset changes.
    /// `NO_SANITIZE_UNDEFINED` because the addition can overflow `Time` for out-of-range arguments
    /// (e.g. a `DateTime64` close to the limits of `Int64`); the wrapped-around result is meaningless
    /// but harmless, like for the `add*` functions below.
    NO_SANITIZE_UNDEFINED ALWAYS_INLINE Time toRelativeHourNum(Time t) const
    {
        if (t >= 0 && offset_is_whole_number_of_hours_during_epoch)
            return t / 3600;

        /// Assume that if offset was fractional, then the fraction is the same as at the beginning of epoch.
        /// NOTE This assumption is false for "Pacific/Pitcairn" and "Pacific/Kiritimati" time zones.
        /// Sum in UInt64 to avoid signed overflow UB on extreme t; non-negative for any representable time, so the result is unchanged.
        return static_cast<Time>(static_cast<UInt64>(t) + DATE_LUT_ADD + 86400 - offset_at_start_of_epoch) / 3600 - (DATE_LUT_ADD / 3600);
    }

    template <typename DateOrTime>
    ALWAYS_INLINE Time toRelativeHourNum(DateOrTime v) const
    {
        return toRelativeHourNum(getValues(v).date);
    }

    /// The same formula is used for positive time (after Unix epoch) and negative time (before Unix epoch).
    /// It's needed for correct work of dateDiff function.
    NO_SANITIZE_UNDEFINED Time toStableRelativeHourNum(Time t) const
    {
        /// Sum in UInt64 to avoid signed overflow UB on extreme t; non-negative for any representable time, so the result is unchanged.
        return static_cast<Time>(static_cast<UInt64>(t) + DATE_LUT_ADD + 86400 - offset_at_start_of_epoch) / 3600 - (DATE_LUT_ADD / 3600);
    }

    template <typename DateOrTime>
    Time toStableRelativeHourNum(DateOrTime v) const
    {
        return toStableRelativeHourNum(getValues(v).date);
    }

    NO_SANITIZE_UNDEFINED Time toRelativeMinuteNum(Time t) const /// NOLINT
    {
        /// Sum in UInt64 to avoid signed overflow UB on extreme t; non-negative for any representable time, so the result is unchanged.
        return static_cast<Time>(static_cast<UInt64>(t) + DATE_LUT_ADD) / 60 - (DATE_LUT_ADD / 60);
    }

    template <typename DateOrTime>
    Time toRelativeMinuteNum(DateOrTime v) const
    {
        return toRelativeMinuteNum(getValues(v).date);
    }

    template <typename DateOrTime>
    auto toStartOfYearInterval(DateOrTime v, UInt64 years) const
    {
        if (years == 1)
            return toFirstDayNumOfYear(v);

        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(v)))
            {
                const Int64 rounded_year = static_cast<Int64>(outOfRangeValues(v).year) / static_cast<Int64>(years) * static_cast<Int64>(years);
                return makeDayNumOutOfRange(rounded_year, 1, 1);
            }

        const LUTIndex i = toLUTIndex(v);

        UInt16 year = static_cast<UInt16>(static_cast<UInt64>(lut[i].year) / years * years);

        /// For example, rounding down 1925 to 100 years will be 1900, but it's less than min supported year.
        if (unlikely(year < DATE_LUT_MIN_YEAR))
            year = DATE_LUT_MIN_YEAR;

        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
            return toDayNum(LUTIndexWithSaturation(years_lut[year - DATE_LUT_MIN_YEAR]));
        else
            return toDayNum(years_lut[year - DATE_LUT_MIN_YEAR]);
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    auto toStartOfQuarterInterval(Date d, UInt64 quarters) const
    {
        if (quarters == 1)
            return toFirstDayNumOfQuarter(d);
        return toStartOfMonthInterval(d, quarters * 3);
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    auto toStartOfMonthInterval(Date d, UInt64 months) const
    {
        if (months == 1)
            return toFirstDayNumOfMonth(d);

        if constexpr (std::is_same_v<Date, ExtendedDayNum>)
            if (unlikely(isOutOfLUTRange(d)))
            {
                /// Number of months since DATE_LUT_MIN_YEAR-01, rounded down to a multiple of `months`.
                const Values values = outOfRangeValues(d);
                const Int64 rel = (static_cast<Int64>(values.year) - DATE_LUT_MIN_YEAR) * 12 + (values.month - 1);
                const Int64 div = static_cast<Int64>(months);
                const Int64 rounded = (rel >= 0 ? rel / div : -((-rel + div - 1) / div)) * div;
                const Int64 absolute_month = static_cast<Int64>(DATE_LUT_MIN_YEAR) * 12 + rounded;
                /// Floor-divide so the month stays in [1, 12] for a negative absolute month (the year is saturated
                /// in makeDayNumOutOfRange); a plain `% 12` would be negative and wrap when cast to UInt8.
                const Int64 abs_year = absolute_month >= 0 ? absolute_month / 12 : -((-absolute_month + 11) / 12);
                const Int64 abs_month = absolute_month - abs_year * 12;
                return makeDayNumOutOfRange(abs_year, static_cast<UInt8>(abs_month) + 1, 1);
            }

        const Values & values = lut[toLUTIndex(d)];
        UInt32 month_total_index = (values.year - DATE_LUT_MIN_YEAR) * 12 + values.month - 1;
        if constexpr (std::is_same_v<Date, DayNum>)
            return toDayNum(LUTIndexWithSaturation(years_months_lut[month_total_index / months * months]));
        else
            return toDayNum(years_months_lut[month_total_index / months * months]);
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    auto toStartOfWeekInterval(Date d, UInt64 weeks) const
    {
        if (weeks == 1)
            return toFirstDayNumOfWeek(d);
        UInt64 days = weeks * 7;
        // January 1st 1970 was Thursday so we need this 4-days offset to make weeks start on Monday.
        if constexpr (std::is_same_v<Date, DayNum>)
            return DayNum(static_cast<UInt16>(4 + (d - 4) / days * days));
        else
        {
            /// Floor towards -inf so a pre-1970 day number rounds to the start of its interval, not forward in time.
            const Int64 idays = static_cast<Int64>(days);
            const Int64 shifted = static_cast<Int64>(d.toUnderType()) - 4;
            const Int64 rounded = (shifted >= 0 ? shifted / idays : (shifted + 1 - idays) / idays) * idays;
            return ExtendedDayNum(static_cast<Int32>(4 + rounded));
        }
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    Time toStartOfDayInterval(Date d, UInt64 days) const
    {
        if (days == 1)
            return toDate(d);

        if constexpr (std::is_same_v<Date, ExtendedDayNum>)
            if (unlikely(isOutOfLUTRange(d)))
            {
                /// Floor division towards -inf: truncating division would round a pre-epoch day number
                /// towards zero, i.e. forward in time, past the start of the interval.
                const Int64 day_num = static_cast<Int64>(d.toUnderType());
                const Int64 idays = static_cast<Int64>(days);
                const Int64 rounded_day_num = day_num >= 0 ? day_num / idays * idays : (day_num + 1 - idays) / idays * idays;
                return dateOfDayIndex(rounded_day_num + daynum_offset_epoch);
            }

        if constexpr (std::is_same_v<Date, DayNum>)
            return lut_saturated[toLUTIndex(ExtendedDayNum(static_cast<Int32>(d / days * days)))].date;
        else
            return lut[toLUTIndex(ExtendedDayNum(static_cast<Int32>(d / days * days)))].date;
    }

    template <typename DateOrTime>
    DateOrTime toStartOfHourInterval(DateOrTime t, UInt64 hours) const
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

        /// `hours` is only validated to be positive by the caller, so an extreme interval count can make
        /// `hours * 3600` wrap. When it wraps to exactly zero (e.g. `toIntervalHour(4611686018427387904)`),
        /// the division by `seconds` below would be undefined behaviour. Saturate the divisor instead; the
        /// rounding result for such meaningless interval counts is discarded anyway.
        UInt64 seconds = 0;
        if (unlikely(__builtin_mul_overflow(hours, static_cast<UInt64>(3600), &seconds)))
            seconds = std::numeric_limits<UInt64>::max();

        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(t)))
            {
                /// In the out-of-range years we ignore the (extrapolated) intra-day offset changes.
                const Values values = outOfRangeValues(t);
                const Time time = (static_cast<Time>(t) - values.date) / static_cast<Time>(seconds) * static_cast<Time>(seconds);
                return static_cast<DateOrTime>(values.date + time);
            }

        const LUTIndex index = findIndexInRange(t);
        const Values & values = lut[index];

        Time time = t - values.date;
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
                time = std::max<Time>(time, values.time_at_offset_change());
            }
        }
        else
        {
            time = time / seconds * seconds;
        }

        Time res = addSaturating(values.date, time);
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
        {
            if (unlikely(res < 0))
                return 0;
            return static_cast<DateOrTime>(res);
        }
        else
            return res;
    }

    template <typename DateOrTime>
    DateOrTime toStartOfMinuteInterval(DateOrTime t, UInt64 minutes) const
    {
        /// `minutes` is only validated to be positive by the caller, so an extreme interval count can make
        /// `60 * minutes` wrap, exactly as in `toStartOfHourInterval`. `INTERVAL 4611686018427387904 MINUTE`
        /// wraps the product to exactly zero, which would then divide by zero in `roundDownToMultiple` (or in
        /// the reconstruction below) before producing a result. Saturate the divisor to the maximum instead;
        /// the rounding result for such meaningless interval counts is discarded anyway.
        UInt64 product = 0;
        if (unlikely(__builtin_mul_overflow(minutes, static_cast<UInt64>(60), &product)
                     || product > static_cast<UInt64>(std::numeric_limits<Int64>::max())))
            product = static_cast<UInt64>(std::numeric_limits<Int64>::max());
        Int64 divisor = static_cast<Int64>(product);

        /// Checked before the fast path below: historical (pre-1900) offsets can have a sub-minute component,
        /// so for out-of-range values the fast path would round to a UTC boundary instead of the local one.
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
            if (unlikely(isOutOfLUTRange(t)))
            {
                const Time date = outOfRangeValues(t).date;
                return static_cast<DateOrTime>(date + (static_cast<Time>(t) - date) / divisor * divisor);
            }

        if (offset_is_whole_number_of_minutes_during_epoch) [[likely]]
            return roundDownToMultiple(t, divisor);

        const Time date = find(t).date;
        Time res = date + (t - date) / divisor * divisor;
        if constexpr (std::is_unsigned_v<DateOrTime> || std::is_same_v<DateOrTime, DayNum>)
        {
            if (unlikely(res < 0))
                return 0;
            return static_cast<UInt32>(res);
        }
        else
            return res;
    }

    template <typename DateOrTime>
    DateOrTime toStartOfSecondInterval(DateOrTime t, UInt64 seconds) const
    {
        if (seconds == 1)
            return t;
        if (seconds % 60 == 0)
            return toStartOfMinuteInterval(t, seconds / 60);

        return static_cast<DateOrTime>(roundDown(t, seconds));
    }

    LUTIndex makeLUTIndex(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return LUTIndex(0);

        if (unlikely(year > DATE_LUT_MAX_YEAR))
            return LUTIndex(DATE_LUT_SIZE - 1);

        auto year_lut_index = (year - DATE_LUT_MIN_YEAR) * 12 + month - 1;
        UInt32 index = years_months_lut[year_lut_index].toUnderType() + day_of_month - 1;
        /// When date is out of range, default value is DATE_LUT_SIZE - 1 (2299-12-31)
        return LUTIndex{std::min(index, static_cast<UInt32>(DATE_LUT_SIZE - 1))};
    }

    std::optional<LUTIndex> tryToMakeLUTIndex(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return std::nullopt;

        auto year_lut_index = (year - DATE_LUT_MIN_YEAR) * 12 + month - 1;
        UInt32 index = years_months_lut[year_lut_index].toUnderType() + day_of_month - 1;

        if (index >= DATE_LUT_SIZE)
            return std::nullopt;

        return LUTIndex(index);
    }

    /// Create DayNum from year, month, day of month.
    ExtendedDayNum makeDayNum(Int16 year, UInt8 month, UInt8 day_of_month, Int32 default_error_day_num = 0) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return ExtendedDayNum(default_error_day_num);

        return toDayNum(makeLUTIndex(year, month, day_of_month));
    }

    std::optional<ExtendedDayNum> tryToMakeDayNum(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (unlikely(year < DATE_LUT_MIN_YEAR || month < 1 || month > 12 || day_of_month < 1 || day_of_month > 31))
            return std::nullopt;

        auto index = tryToMakeLUTIndex(year, month, day_of_month);
        if (!index)
            return std::nullopt;

        return toDayNum(*index);
    }

    Time makeDate(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        return lut[makeLUTIndex(year, month, day_of_month)].date;
    }

    /// Whether the given year/month/day fall outside of the lookup table but still form a valid calendar date.
    static bool isMakeDateOutOfRange(Int16 year, UInt8 month, UInt8 day_of_month)
    {
        return (year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR)
            && month >= 1 && month <= 12 && day_of_month >= 1 && day_of_month <= 31;
    }

    /** Does not accept daylight saving time as argument: in case of ambiguity, it choose greater timestamp.
      */
    Time makeDateTime(Int16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const
    {
        if (unlikely(isMakeDateOutOfRange(year, month, day_of_month)))
            return makeDateTimeOutOfRange(year, month, day_of_month, hour, minute, second);

        size_t index = makeLUTIndex(year, month, day_of_month);
        Time time_offset = hour * 3600 + minute * 60 + second;

        if (time_offset >= lut[index].time_at_offset_change())
            time_offset -= lut[index].amount_of_offset_change();

        return lut[index].date + time_offset;
    }

    std::optional<Time> tryToMakeDateTime(Int16 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const
    {
        if (unlikely(isMakeDateOutOfRange(year, month, day_of_month)))
            return makeDateTimeOutOfRange(year, month, day_of_month, hour, minute, second);

        auto index = tryToMakeLUTIndex(year, month, day_of_month);
        if (!index)
            return std::nullopt;

        Time time_offset = hour * 3600 + minute * 60 + second;

        if (time_offset >= lut[*index].time_at_offset_change())
            time_offset -= lut[*index].amount_of_offset_change();

        return lut[*index].date + time_offset;
    }

    Time makeTime(int64_t hour, UInt8 minute, UInt8 second) const
    {
        Time time_offset = hour * 3600 + minute * 60 + second;

        if (time_offset >= lut[1].time_at_offset_change())
            time_offset -= lut[0].amount_of_offset_change();

        return time_offset;
    }

    /// Returns the Values of the day the argument belongs to. Returns by value so the out-of-range escape path
    /// (which computes the Values with cctz) can be used; for in-range values the copy is elided by the optimizer.
    template <typename DateOrTime>
    Values getValues(DateOrTime v) const
    {
        if constexpr (may_be_out_of_lut_range<DateOrTime>)
        {
            if (unlikely(isOutOfLUTRange(v)))
                return outOfRangeValues(v);
            /// Already gated: skip the redundant bound clamp that findIndex would repeat.
            if constexpr (std::is_same_v<DateOrTime, Time>)
                return lut[findIndexInRange(v)];
        }
        return lut[toLUTIndex(v)];
    }

    template <typename DateOrTime>
    UInt32 toNumYYYYMM(DateOrTime v) const
    {
        const Values & values = getValues(v);
        return values.year * 100 + values.month;
    }

    template <typename DateOrTime>
    UInt32 toNumYYYYMMDD(DateOrTime v) const
    {
        const Values & values = getValues(v);
        return values.year * 10000 + values.month * 100 + values.day_of_month;
    }

    Time YYYYMMDDToDate(UInt32 num) const /// NOLINT
    {
        return makeDate(static_cast<Int16>(num / 10000), static_cast<UInt8>(num / 100 % 100), static_cast<UInt8>(num % 100));
    }

    ExtendedDayNum YYYYMMDDToDayNum(UInt32 num) const /// NOLINT
    {
        return makeDayNum(static_cast<Int16>(num / 10000), static_cast<UInt8>(num / 100 % 100), static_cast<UInt8>(num % 100));
    }


    struct DateComponents
    {
        uint16_t year;
        uint8_t month;
        uint8_t day;
    };

    struct TimeComponents
    {
        bool is_negative = false;
        uint64_t hour{};
        uint8_t minute{};
        uint8_t second{};
    };

    struct DateTimeComponents
    {
        DateComponents date{};
        TimeComponents time;
    };

private:
    /// Escape paths implemented with cctz (defined in DateLUTImpl.cpp). All of them are only reached for
    /// values outside of [DATE_LUT_MIN_YEAR, DATE_LUT_MAX_YEAR] and are guarded by `unlikely` at the call sites.
    Int64 findDayIndexOutOfRange(Time t) const;
    Values valuesForOutOfRangeDayIndex(Int64 day_index) const;
    DateTimeComponents toDateTimeComponentsOutOfRange(Time t) const;
    Time toTimeOutOfRange(Time t) const;
    Time timezoneOffsetOutOfRange(Time t) const;
    Time addDaysOutOfRange(Time t, Int64 delta) const;
    Time addMonthsOutOfRange(Time t, Int64 delta) const;
    Time addYearsOutOfRange(Time t, Int64 delta) const;
    ExtendedDayNum addMonthsOutOfRange(ExtendedDayNum d, Int64 delta) const;
    ExtendedDayNum addYearsOutOfRange(ExtendedDayNum d, Int64 delta) const;
    Time makeDateOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month) const;
    Time makeDateTimeOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month, UInt8 hour, UInt8 minute, UInt8 second) const;
    ExtendedDayNum makeDayNumOutOfRange(Int64 year, UInt8 month, UInt8 day_of_month) const;

public:
    DateComponents toDateComponents(Time t) const
    {
        const Values & values = getValues(t);
        return { .year = values.year, .month = values.month, .day = values.day_of_month };
    }

    DateTimeComponents toDateTimeComponents(Time t) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return toDateTimeComponentsOutOfRange(t);

        const LUTIndex index = findIndexInRange(t);
        const Values & values = lut[index];

        DateTimeComponents res;

        res.date.year = values.year;
        res.date.month = values.month;
        res.date.day = values.day_of_month;

        Time time = t - values.date;
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

    TimeComponents toTimeComponents(Time t) const
    {
        TimeComponents res;

        bool is_negative = false;

        if (unlikely(t < 0))
        {
            is_negative = true;
            t = -t;
        }

        // Cap at 3599999 seconds (999:59:59)
        if (unlikely(t > 3599999))
            t = 3599999;

        res.second = t % 60;
        res.minute = t / 60 % 60;
        res.hour = t / 3600;

        res.is_negative = is_negative;

        return res;
    }

    template <typename DateOrTime>
    DateTimeComponents toDateTimeComponents(DateOrTime v) const
    {
        return toDateTimeComponents(getValues(v).date);
    }

    UInt64 toNumYYYYMMDDhhmmss(Time t) const
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

    Time YYYYMMDDhhmmssToTime(UInt64 num) const /// NOLINT
    {
        return makeDateTime(
            static_cast<Int16>(num / 10000000000),
            num / 100000000 % 100,
            num / 1000000 % 100,
            num / 10000 % 100,
            num / 100 % 100,
            num % 100);
    }

    /// Adding calendar intervals.
    /// Implementation specific behaviour when delta is too big.

    NO_SANITIZE_UNDEFINED Time addDays(Time t, Int64 delta) const
    {
        if (unlikely(isOutOfLUTRange(t)))
            return addDaysOutOfRange(t, delta);

        const LUTIndex index = findIndexInRange(t);

        /// If the destination day falls outside of the lookup table, recompute everything with cctz.
        /// Compare delta against the valid window so the bound check itself cannot overflow for huge delta.
        const Int64 index_value = static_cast<Int64>(index.toUnderType());
        if (unlikely(delta < -index_value || delta >= static_cast<Int64>(DATE_LUT_SIZE) - index_value))
            return addDaysOutOfRange(t, delta);

        const Values & values = lut[index];

        Time time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        const LUTIndex new_index = index + delta;

        if (time >= lut[new_index].time_at_offset_change())
            time -= lut[new_index].amount_of_offset_change();

        return lut[new_index].date + time;
    }

    NO_SANITIZE_UNDEFINED Time addWeeks(Time t, Int64 delta) const
    {
        return addDays(t, delta * 7);
    }

    UInt8 saturateDayOfMonth(Int16 year, UInt8 month, UInt8 day_of_month) const
    {
        if (day_of_month <= 28) [[likely]]
            return day_of_month;

        UInt8 days_in_month = daysInMonth(year, month);

        return std::min(day_of_month, days_in_month);
    }

    template <typename DateOrTime>
    LUTIndex NO_SANITIZE_UNDEFINED addMonthsIndex(DateOrTime v, Int64 delta) const
    {
        const Values & values = lut[toLUTIndex(v)];

        Int64 month = values.month + delta;

        if (month > 0)
        {
            auto year = values.year + (month - 1) / 12;
            month = ((month - 1) % 12) + 1;
            auto day_of_month = saturateDayOfMonth(static_cast<Int16>(year), static_cast<UInt8>(month), values.day_of_month);

            return makeLUTIndex(static_cast<Int16>(year), static_cast<UInt8>(month), day_of_month);
        }

        auto year = values.year - (12 - month) / 12;
        month = 12 - (-month % 12);
        auto day_of_month = saturateDayOfMonth(static_cast<Int16>(year), static_cast<UInt8>(month), values.day_of_month);

        return makeLUTIndex(static_cast<Int16>(year), static_cast<UInt8>(month), day_of_month);
    }

    /// If resulting month has less days than source month, then saturation can happen.
    /// Example: 31 Aug + 1 month = 30 Sep.
    template <typename DateTime>
    requires std::is_same_v<DateTime, UInt32> || std::is_same_v<DateTime, Int64> || std::is_same_v<DateTime, time_t>
    Time NO_SANITIZE_UNDEFINED addMonths(DateTime t, Int64 delta) const
    {
        /// DateTime (UInt32) cannot represent values outside of the lookup table, so only DateTime64 takes the escape path.
        if constexpr (!std::is_same_v<DateTime, UInt32>)
            if (unlikely(isOutOfLUTRange(static_cast<Time>(t))))
                return addMonthsOutOfRange(t, delta);

        const LUTIndex index = findIndexInRange(t);
        const Values & values = lut[index];

        if constexpr (!std::is_same_v<DateTime, UInt32>)
        {
            /// If the destination month falls outside of the lookup table, recompute everything with cctz.
            /// Compare delta against the valid window so the bound check itself cannot overflow for huge delta.
            const Int64 base_month_index = static_cast<Int64>(values.year) * 12 + (values.month - 1);
            if (unlikely(delta < static_cast<Int64>(DATE_LUT_MIN_YEAR) * 12 - base_month_index
                      || delta > static_cast<Int64>(DATE_LUT_MAX_YEAR) * 12 + 11 - base_month_index))
                return addMonthsOutOfRange(t, delta);
        }

        const auto result_day = addMonthsIndex(t, delta);

        Time time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        if (time >= lut[result_day].time_at_offset_change())
            time -= lut[result_day].amount_of_offset_change();

        auto res = lut[result_day].date + time;
        if constexpr (std::is_same_v<DateTime, UInt32>)
        {
            /// Common compiler should generate branchless code for this saturation operation.
            return res <= 0 ? 0 : res;
        }
        else
            return res;
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    auto NO_SANITIZE_UNDEFINED addMonths(Date d, Int64 delta) const
    {
        if constexpr (std::is_same_v<Date, DayNum>)
            return toDayNum(LUTIndexWithSaturation(addMonthsIndex(d, delta)));
        else
        {
            if (unlikely(isOutOfLUTRange(d)))
                return addMonthsOutOfRange(d, delta);

            const Values & values = lut[toLUTIndex(d)];
            /// Compare delta against the valid window so the bound check itself cannot overflow for huge delta.
            const Int64 base_month_index = static_cast<Int64>(values.year) * 12 + (values.month - 1);
            if (unlikely(delta < static_cast<Int64>(DATE_LUT_MIN_YEAR) * 12 - base_month_index
                      || delta > static_cast<Int64>(DATE_LUT_MAX_YEAR) * 12 + 11 - base_month_index))
                return addMonthsOutOfRange(d, delta);

            return toDayNum(addMonthsIndex(d, delta));
        }
    }

    template <typename DateOrTime>
    auto NO_SANITIZE_UNDEFINED addQuarters(DateOrTime d, Int64 delta) const
    {
        return addMonths(d, delta * 3);
    }

    template <typename DateOrTime>
    LUTIndex NO_SANITIZE_UNDEFINED addYearsIndex(DateOrTime v, Int64 delta) const
    {
        const Values & values = lut[toLUTIndex(v)];

        auto year = values.year + static_cast<Int16>(delta);
        auto month = values.month;
        auto day_of_month = values.day_of_month;

        /// Saturation to 28 Feb can happen.
        if (unlikely(day_of_month == 29 && month == 2))
            day_of_month = saturateDayOfMonth(static_cast<Int16>(year), month, day_of_month);

        return makeLUTIndex(static_cast<Int16>(year), month, day_of_month);
    }

    /// Saturation can occur if 29 Feb is mapped to non-leap year.
    template <typename DateTime>
    requires std::is_same_v<DateTime, UInt32> || std::is_same_v<DateTime, Int64> || std::is_same_v<DateTime, time_t>
    Time addYears(DateTime t, Int64 delta) const
    {
        /// DateTime (UInt32) cannot represent values outside of the lookup table, so only DateTime64 takes the escape path.
        if constexpr (!std::is_same_v<DateTime, UInt32>)
            if (unlikely(isOutOfLUTRange(static_cast<Time>(t))))
                return addYearsOutOfRange(t, delta);

        const LUTIndex index = findIndexInRange(t);
        const Values & values = lut[index];

        if constexpr (!std::is_same_v<DateTime, UInt32>)
        {
            /// Compare delta against the valid window so the bound check itself cannot overflow for huge delta.
            if (unlikely(delta < static_cast<Int64>(DATE_LUT_MIN_YEAR) - values.year
                      || delta > static_cast<Int64>(DATE_LUT_MAX_YEAR) - values.year))
                return addYearsOutOfRange(t, delta);
        }

        auto result_day = addYearsIndex(t, delta);

        Time time = t - values.date;
        if (time >= values.time_at_offset_change())
            time += values.amount_of_offset_change();

        if (time >= lut[result_day].time_at_offset_change())
            time -= lut[result_day].amount_of_offset_change();

        auto res = lut[result_day].date + time;
        if constexpr (std::is_same_v<DateTime, UInt32>)
        {
            /// Common compiler should generate branchless code for this saturation operation.
            return res <= 0 ? 0 : res;
        }
        else
            return res;
    }

    template <typename Date>
    requires std::is_same_v<Date, DayNum> || std::is_same_v<Date, ExtendedDayNum>
    auto addYears(Date d, Int64 delta) const
    {
        if constexpr (std::is_same_v<Date, DayNum>)
            return toDayNum(LUTIndexWithSaturation(addYearsIndex(d, delta)));
        else
        {
            if (unlikely(isOutOfLUTRange(d)))
                return addYearsOutOfRange(d, delta);

            const Values & values = lut[toLUTIndex(d)];
            /// Compare delta against the valid window so the bound check itself cannot overflow for huge delta.
            if (unlikely(delta < static_cast<Int64>(DATE_LUT_MIN_YEAR) - values.year
                      || delta > static_cast<Int64>(DATE_LUT_MAX_YEAR) - values.year))
                return addYearsOutOfRange(d, delta);

            return toDayNum(addYearsIndex(d, delta));
        }
    }


    std::string timeToString(Time t) const
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

    std::string dateToString(Time t) const
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

    std::string dateToString(ExtendedDayNum d) const
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
