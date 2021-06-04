#pragma once

#include <common/types.h>
#include <Core/DecimalFunctions.h>
#include <Common/Exception.h>
#include <common/DateLUTImpl.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/** Transformations.
  * Represents two functions - from datetime (UInt32) and from date (UInt16).
  *
  * Also, the "factor transformation" F is defined for the T transformation.
  * This is a transformation of F such that its value identifies the region of monotonicity for T
  *  (for a fixed value of F, the transformation T is monotonic).
  *
  * Or, figuratively, if T is similar to taking the remainder of division, then F is similar to division.
  *
  * Example: for transformation T "get the day number in the month" (2015-02-03 -> 3),
  *  factor-transformation F is "round to the nearest month" (2015-02-03 -> 2015-02-01).
  */

    static inline UInt32 dateIsNotSupported(const char * name)
    {
        throw Exception("Illegal type Date of argument for function " + std::string(name), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(Int64, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt32, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, const DateLUTImpl &) { return 0; }
};

struct ToDateImpl
{
    static constexpr auto name = "toDate";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
    static constexpr auto name = "toStartOfDay";

    //TODO: right now it is hardcoded to produce DateTime only, needs fixing later. See date_and_time_type_details::ResultDataTypeMap for deduction of result type example.
    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDate(static_cast<time_t>(t.whole));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDate(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDate(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToMondayImpl
{
    static constexpr auto name = "toMonday";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        //return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
        return time_zone.toFirstDayNumOfWeek(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        //return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
        return time_zone.toFirstDayNumOfWeek(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMonthImpl
{
    static constexpr auto name = "toStartOfMonth";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfQuarterImpl
{
    static constexpr auto name = "toStartOfQuarter";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfYearImpl
{
    static constexpr auto name = "toStartOfYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
    /// When transforming to time, the date will be equated to 1970-01-01.
    static constexpr auto name = "toTime";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toTime(t.whole) + 86400;
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toTime(t) + 86400;
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t.whole);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

// Rounding towards negative infinity.
// 1.01 => 1.00
// -1.01 => -2
struct ToStartOfSecondImpl
{
    static constexpr auto name = "toStartOfSecond";

    static inline DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        auto fractional_with_sign = DecimalUtils::getFractionalPartWithScaleMultiplier<DateTime64, true>(datetime64, scale_multiplier);

        // given that scale is 3, scale_multiplier is 1000
        // for DateTime64 value of 123.456:
        // 123456 - 456 = 123000
        // for DateTime64 value of -123.456:
        // -123456 - (1000 + (-456)) = -124000

        if (fractional_with_sign < 0)
            fractional_with_sign += scale_multiplier;

        return datetime64 - fractional_with_sign;
    }

    static inline UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception("Illegal type DateTime of argument for function " + std::string(name), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinuteImpl
{
    static constexpr auto name = "toStartOfFiveMinute";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinute(t.whole);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfTenMinutesImpl
{
    static constexpr auto name = "toStartOfTenMinutes";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfTenMinutes(t.whole);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfTenMinutes(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFifteenMinutesImpl
{
    static constexpr auto name = "toStartOfFifteenMinutes";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t.whole);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

/// Round to start of half-an-hour length interval with unspecified offset. This transform is specific for Yandex.Metrica.
struct TimeSlotImpl
{
    static constexpr auto name = "timeSlot";

    //static inline DecimalUtils::DecimalComponents<DateTime64> execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    {
        return t.whole / 1800 * 1800;
    }

    static inline UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t / 1800 * 1800;
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t.whole);
    }

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t);
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
    static constexpr auto name = "toYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToQuarterImpl
{
    static constexpr auto name = "toQuarter";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(ExtendedDayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToMonthImpl
{
    static constexpr auto name = "toMonth";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(ExtendedDayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
    static constexpr auto name = "toDayOfMonth";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(ExtendedDayNum(d));
    }

    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(ExtendedDayNum(d));
    }

    using FactorTransform = ToMondayImpl;
};

struct ToDayOfYearImpl
{
    static constexpr auto name = "toDayOfYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(ExtendedDayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToHourImpl
{
    static constexpr auto name = "toHour";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }

    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ToDateImpl;
};

struct TimezoneOffsetImpl
{
    static constexpr auto name = "timezoneOffset";

    static inline time_t execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.timezoneOffset(t);
    }

    static inline time_t execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.timezoneOffset(t);
    }

    static inline time_t execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ToTimeImpl;
};

struct ToMinuteImpl
{
    static constexpr auto name = "toMinute";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
    static constexpr auto name = "toSecond";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        return dateIsNotSupported(name);
    }

    using FactorTransform = ToStartOfMinuteImpl;
};

struct ToISOYearImpl
{
    static constexpr auto name = "toISOYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfISOYearImpl
{
    static constexpr auto name = "toStartOfISOYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToISOWeekImpl
{
    static constexpr auto name = "toISOWeek";

    static inline UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(ExtendedDayNum(d));
    }

    using FactorTransform = ToISOYearImpl;
};

struct ToRelativeYearNumImpl
{
    static constexpr auto name = "toRelativeYearNum";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(static_cast<time_t>(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(static_cast<time_t>(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(static_cast<time_t>(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(static_cast<time_t>(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(static_cast<time_t>(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return static_cast<DayNum>(d);
    }

    using FactorTransform = ZeroTransform;
};


struct ToRelativeHourNumImpl
{
    static constexpr auto name = "toRelativeHourNum";

    static inline UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(t);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(static_cast<time_t>(t));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static inline UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(t);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(static_cast<time_t>(t));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeSecondNumImpl
{
    static constexpr auto name = "toRelativeSecondNum";

    static inline Int64 execute(Int64 t, const DateLUTImpl &)
    {
        return t;
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t;
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMImpl
{
    static constexpr auto name = "toYYYYMM";

    static inline UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(t);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(static_cast<DayNum>(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDImpl
{
    static constexpr auto name = "toYYYYMMDD";

    static inline UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(t);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(static_cast<DayNum>(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDhhmmssImpl
{
    static constexpr auto name = "toYYYYMMDDhhmmss";

    static inline UInt64 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(t);
    }
    static inline UInt64 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(t);
    }
    static inline UInt64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(static_cast<DayNum>(d)));
    }

    using FactorTransform = ZeroTransform;
};


template <typename FromType, typename ToType, typename Transform>
struct Transformer
{
    template <typename FromTypeVector, typename ToTypeVector>
    static void vector(const FromTypeVector & vec_from, ToTypeVector & vec_to, const DateLUTImpl & time_zone, const Transform & transform)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = transform.execute(vec_from[i], time_zone);
    }
};


template <typename FromDataType, typename ToDataType, typename Transform>
struct DateTimeTransformImpl
{
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/, const Transform & transform = {})
    {
        using Op = Transformer<typename FromDataType::FieldType, typename ToDataType::FieldType, Transform>;

        size_t time_zone_argument_position = 1;
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
            time_zone_argument_position = 2;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, time_zone_argument_position, 0);

        const ColumnPtr source_col = arguments[0].column;
        if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            auto mutable_result_col = result_type->createColumn();
            auto * col_to = assert_cast<typename ToDataType::ColumnType *>(mutable_result_col.get());

            Op::vector(sources->getData(), col_to->getData(), time_zone, transform);

            return mutable_result_col;
        }
        else
        {
            throw Exception("Illegal column " + arguments[0].column->getName()
                + " of first argument of function " + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

}
