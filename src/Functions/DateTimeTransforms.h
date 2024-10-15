#pragma once

#include <base/arithmeticOverflow.h>
#include <base/types.h>
#include <Core/DecimalFunctions.h>
#include <Common/Exception.h>
#include <Common/DateLUTImpl.h>
#include <Common/DateLUT.h>
#include <Common/IntervalKind.h>
#include "base/Decimal.h"
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

static constexpr auto millisecond_multiplier = 1'000;
static constexpr auto microsecond_multiplier = 1'000'000;
static constexpr auto nanosecond_multiplier = 1'000'000'000;

static constexpr FormatSettings::DateTimeOverflowBehavior default_date_time_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Ignore;

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int DECIMAL_OVERFLOW;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
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

constexpr time_t MAX_DATETIME64_TIMESTAMP = 10413791999LL;    //  1900-01-01 00:00:00 UTC
constexpr time_t MIN_DATETIME64_TIMESTAMP = -2208988800LL;    //  2299-12-31 23:59:59 UTC
constexpr time_t MAX_DATETIME_TIMESTAMP = 0xFFFFFFFF;
constexpr time_t MAX_DATE_TIMESTAMP = 5662310399;       // 2149-06-06 23:59:59 UTC
constexpr time_t MAX_DATETIME_DAY_NUM =  49710;               // 2106-02-07

[[noreturn]] void throwDateIsNotSupported(const char * name);
[[noreturn]] void throwDate32IsNotSupported(const char * name);
[[noreturn]] void throwDateTimeIsNotSupported(const char * name);

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static UInt16 execute(Int64, const DateLUTImpl &) { return 0; }
    static UInt16 execute(UInt32, const DateLUTImpl &) { return 0; }
    static UInt16 execute(Int32, const DateLUTImpl &) { return 0; }
    static UInt16 execute(UInt16, const DateLUTImpl &) { return 0; }
};

template <FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior = default_date_time_overflow_behavior>
struct ToDateImpl
{
    static constexpr auto name = "toDate";

    static UInt16 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return execute(t.whole, time_zone);
    }

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
        {
            if (t < 0)
                t = 0;
            else if (t > MAX_DATE_TIMESTAMP)
                t = MAX_DATE_TIMESTAMP;
        }
        else if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (t < 0 || t > MAX_DATE_TIMESTAMP) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", t);
        }
        return static_cast<UInt16>(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));  /// never causes overflow by design
    }
    static UInt16 execute(Int32 t, const DateLUTImpl &)
    {
        if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Saturate)
        {
            if (t < 0)
                return UInt16(0);
            if (t > DATE_LUT_MAX_DAY_NUM)
                return UInt16(DATE_LUT_MAX_DAY_NUM);
        }
        else if constexpr (date_time_overflow_behavior == FormatSettings::DateTimeOverflowBehavior::Throw)
        {
            if (t < 0 || t > DATE_LUT_MAX_DAY_NUM) [[unlikely]]
                throw Exception(ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, "Value {} is out of bounds of type Date", t);
        }
        return static_cast<UInt16>(t);
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toDayNum(t.whole), 0};
    }

    using FactorTransform = ZeroTransform;
};

struct ToDate32Impl
{
    static constexpr auto name = "toDate32";

    static Int32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return Int32(time_zone.toDayNum(t));
    }
    static Int32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        /// Don't saturate.
        return Int32(time_zone.toDayNum<Int64>(t));
    }
    static Int32 execute(Int32 d, const DateLUTImpl &)
    {
        return d;
    }
    static Int32 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
    static constexpr auto name = "toStartOfDay";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(static_cast<time_t>(t.whole)));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(t));
    }
    static UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(ExtendedDayNum(d)));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(DayNum(d)));
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toDate(t.whole), 0};
    }
    static Int64 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return common::mulIgnoreOverflow(time_zone.fromDayNum(ExtendedDayNum(d)), DecimalUtils::scaleMultiplier<DateTime64>(DataTypeDateTime64::default_scale));
    }

    using FactorTransform = ZeroTransform;
};

struct ToMondayImpl
{
    static constexpr auto name = "toMonday";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        //return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
        return time_zone.toFirstDayNumOfWeek(t);
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        //return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
        return time_zone.toFirstDayNumOfWeek(t);
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d));
    }
    using FactorTransform = ZeroTransform;
};

struct ToStartOfMonthImpl
{
    static constexpr auto name = "toStartOfMonth";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToLastDayOfMonthImpl
{
    static constexpr auto name = "toLastDayOfMonth";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(ExtendedDayNum(d));
    }
    using FactorTransform = ZeroTransform;
};

struct ToStartOfQuarterImpl
{
    static constexpr auto name = "toStartOfQuarter";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(ExtendedDayNum(d));
    }
    using FactorTransform = ZeroTransform;
};

struct ToStartOfYearImpl
{
    static constexpr auto name = "toStartOfYear";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearWeekImpl
{
    static constexpr auto name = "toYearWeek";
    static constexpr bool value_may_be_string = true;

    static UInt32 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        // TODO: ditch toDayNum()
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }

    static UInt32 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }
    static UInt32 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(ExtendedDayNum (d), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }
    static UInt32 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode | static_cast<UInt32>(WeekModeFlag::YEAR));
        return yw.first * 100 + yw.second;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfWeekImpl
{
    static constexpr auto name = "toStartOfWeek";
    static constexpr bool value_may_be_string = false;

    static UInt16 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        const int res = time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
        return std::max(res, 0);
    }
    static UInt16 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        const int res = time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
        return std::max(res, 0);
    }
    static UInt16 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d), week_mode);
    }
    static UInt16 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d), week_mode);
    }
    static Int64 executeExtendedResult(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static Int32 executeExtendedResult(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d), week_mode);
    }

    using FactorTransform = ZeroTransform;
};

struct ToLastDayOfWeekImpl
{
    static constexpr auto name = "toLastDayOfWeek";
    static constexpr bool value_may_be_string = false;

    static UInt16 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static UInt16 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static UInt16 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(ExtendedDayNum(d), week_mode);
    }
    static UInt16 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(DayNum(d), week_mode);
    }
    static Int64 executeExtendedResult(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(time_zone.toDayNum(t), week_mode);
    }
    static Int32 executeExtendedResult(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfWeek(ExtendedDayNum(d), week_mode);
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekImpl
{
    static constexpr auto name = "toWeek";
    static constexpr bool value_may_be_string = true;

    static UInt8 execute(Int64 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        // TODO: ditch conversion to DayNum, since it doesn't support extended range.
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static UInt8 execute(UInt32 t, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(time_zone.toDayNum(t), week_mode);
        return yw.second;
    }
    static UInt8 execute(Int32 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(ExtendedDayNum(d), week_mode);
        return yw.second;
    }
    static UInt8 execute(UInt16 d, UInt8 week_mode, const DateLUTImpl & time_zone)
    {
        YearWeek yw = time_zone.toYearWeek(DayNum(d), week_mode);
        return yw.second;
    }

    using FactorTransform = ToStartOfYearImpl;
};

template <IntervalKind::Kind unit>
struct ToStartOfInterval;

static constexpr auto TO_START_OF_INTERVAL_NAME = "toStartOfInterval";

template <>
struct ToStartOfInterval<IntervalKind::Kind::Nanosecond>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64)
    {
        throwDateTimeIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static Int64 execute(Int64 t, Int64 nanoseconds, const DateLUTImpl &, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        if (scale_multiplier < 1000000000)
        {
            Int64 t_nanoseconds = 0;
            if (common::mulOverflow(t, (static_cast<Int64>(1000000000) / scale_multiplier), t_nanoseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
            if (t >= 0) [[likely]]
                return t_nanoseconds / nanoseconds * nanoseconds;
            else
                return ((t_nanoseconds + 1) / nanoseconds - 1) * nanoseconds;
        }
        else
            if (t >= 0) [[likely]]
                return t / nanoseconds * nanoseconds;
            else
                return ((t + 1) / nanoseconds - 1) * nanoseconds;
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Microsecond>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64)
    {
        throwDateTimeIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static Int64 execute(Int64 t, Int64 microseconds, const DateLUTImpl &, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        if (scale_multiplier < 1000000)
        {
            Int64 t_microseconds = 0;
            if (common::mulOverflow(t, static_cast<Int64>(1000000) / scale_multiplier, t_microseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
            if (t >= 0) [[likely]]
                return t_microseconds / microseconds * microseconds;
            else
                return ((t_microseconds + 1) / microseconds - 1) * microseconds;
        }
        else if (scale_multiplier > 1000000)
        {
            Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000000);
            if (t >= 0) [[likely]] /// When we divide the `t` value we should round the result
                return (t + scale_diff / 2) / (microseconds * scale_diff) * microseconds;
            else
                return ((t + 1) / microseconds / scale_diff - 1) * microseconds;
        }
        else
            if (t >= 0) [[likely]]
                return t / microseconds * microseconds;
            else
                return ((t + 1) / microseconds - 1) * microseconds;
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Millisecond>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64)
    {
        throwDateTimeIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static Int64 execute(Int64 t, Int64 milliseconds, const DateLUTImpl &, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        if (scale_multiplier < 1000)
        {
            Int64 t_milliseconds = 0;
            if (common::mulOverflow(t, static_cast<Int64>(1000) / scale_multiplier, t_milliseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
            if (t >= 0) [[likely]]
                return t_milliseconds / milliseconds * milliseconds;
            else
                return ((t_milliseconds + 1) / milliseconds - 1) * milliseconds;
        }
        else if (scale_multiplier > 1000)
        {
            Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000);
            if (t >= 0) [[likely]]  /// When we divide the `t` value we should round the result
                return (t + scale_diff / 2) / (milliseconds * scale_diff) * milliseconds;
            else
                return ((t + 1) / milliseconds / scale_diff - 1) * milliseconds;
        }
        else
            if (t >= 0) [[likely]]
                return t / milliseconds * milliseconds;
            else
                return ((t + 1) / milliseconds - 1) * milliseconds;
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Second>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32 t, Int64 seconds, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfSecondInterval(t, seconds);
    }
    static Int64 execute(Int64 t, Int64 seconds, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        return time_zone.toStartOfSecondInterval(t / scale_multiplier, seconds);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Minute>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32 t, Int64 minutes, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfMinuteInterval(t, minutes);
    }
    static Int64 execute(Int64 t, Int64 minutes, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        return time_zone.toStartOfMinuteInterval(t / scale_multiplier, minutes);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Hour>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64)
    {
        throwDateIsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64)
    {
        throwDate32IsNotSupported(TO_START_OF_INTERVAL_NAME);
    }
    static UInt32 execute(UInt32 t, Int64 hours, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfHourInterval(t, hours);
    }
    static Int64 execute(Int64 t, Int64 hours, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        return time_zone.toStartOfHourInterval(t / scale_multiplier, hours);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Day>
{
    static UInt32 execute(UInt16 d, Int64 days, const DateLUTImpl & time_zone, Int64)
    {
        return static_cast<UInt32>(time_zone.toStartOfDayInterval(ExtendedDayNum(d), days));
    }
    static UInt32 execute(Int32 d, Int64 days, const DateLUTImpl & time_zone, Int64)
    {
        return static_cast<UInt32>(time_zone.toStartOfDayInterval(ExtendedDayNum(d), days));
    }
    static UInt32 execute(UInt32 t, Int64 days, const DateLUTImpl & time_zone, Int64)
    {
        return static_cast<UInt32>(time_zone.toStartOfDayInterval(time_zone.toDayNum(t), days));
    }
    static Int64 execute(Int64 t, Int64 days, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 /*origin*/ = 0)
    {
        return time_zone.toStartOfDayInterval(time_zone.toDayNum(t / scale_multiplier), days);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Week>
{
    static UInt16 execute(UInt16 d, Int64 weeks, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfWeekInterval(DayNum(d), weeks);
    }
    static UInt16 execute(Int32 d, Int64 weeks, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfWeekInterval(ExtendedDayNum(d), weeks);
    }
    static UInt16 execute(UInt32 t, Int64 weeks, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t), weeks);
    }
    static Int64 execute(Int64 t, Int64 weeks, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 origin = 0)
    {
        if (origin == 0)
            return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t / scale_multiplier), weeks);
        return ToStartOfInterval<IntervalKind::Kind::Day>::execute(t, weeks * 7, time_zone, scale_multiplier, origin);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Month>
{
    static UInt16 execute(UInt16 d, Int64 months, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfMonthInterval(DayNum(d), months);
    }
    static UInt16 execute(Int32 d, Int64 months, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfMonthInterval(ExtendedDayNum(d), months);
    }
    static UInt16 execute(UInt32 t, Int64 months, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfMonthInterval(time_zone.toDayNum(t), months);
    }
    static Int64 execute(Int64 t, Int64 months, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 origin = 0)
    {
        const Int64 scaled_time = t / scale_multiplier;
        if (origin == 0)
            return time_zone.toStartOfMonthInterval(time_zone.toDayNum(scaled_time), months);

        const Int64 scaled_origin = origin / scale_multiplier;
        const Int64 days = time_zone.toDayOfMonth(scaled_time + scaled_origin) - time_zone.toDayOfMonth(scaled_origin);
        Int64 months_to_add = time_zone.toMonth(scaled_time + scaled_origin) - time_zone.toMonth(scaled_origin);
        const Int64 years = time_zone.toYear(scaled_time + scaled_origin) - time_zone.toYear(scaled_origin);
        months_to_add = days < 0 ? months_to_add - 1 : months_to_add;
        months_to_add += years * 12;
        Int64 month_multiplier = (months_to_add / months) * months;

        return (time_zone.addMonths(time_zone.toDate(scaled_origin), month_multiplier) - time_zone.toDate(scaled_origin));
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Quarter>
{
    static UInt16 execute(UInt16 d, Int64 quarters, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfQuarterInterval(DayNum(d), quarters);
    }
    static UInt16 execute(Int32 d, Int64 quarters, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfQuarterInterval(ExtendedDayNum(d), quarters);
    }
    static UInt16 execute(UInt32 t, Int64 quarters, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t), quarters);
    }
    static Int64 execute(Int64 t, Int64 quarters, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 origin = 0)
    {
        if (origin == 0)
            return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t / scale_multiplier), quarters);
        return ToStartOfInterval<IntervalKind::Kind::Month>::execute(t, quarters * 3, time_zone, scale_multiplier, origin);
    }
};

template <>
struct ToStartOfInterval<IntervalKind::Kind::Year>
{
    static UInt16 execute(UInt16 d, Int64 years, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfYearInterval(DayNum(d), years);
    }
    static UInt16 execute(Int32 d, Int64 years, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfYearInterval(ExtendedDayNum(d), years);
    }
    static UInt16 execute(UInt32 t, Int64 years, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfYearInterval(time_zone.toDayNum(t), years);
    }
    static Int64 execute(Int64 t, Int64 years, const DateLUTImpl & time_zone, Int64 scale_multiplier, Int64 origin = 0)
    {
        if (origin == 0)
            return time_zone.toStartOfYearInterval(time_zone.toDayNum(t / scale_multiplier), years);
        return ToStartOfInterval<IntervalKind::Kind::Month>::execute(t, years * 12, time_zone, scale_multiplier, origin);
    }
};


struct ToTimeImpl
{
    /// When transforming to time, the date will be equated to 1970-01-02.
    static constexpr auto name = "toTime";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toTime(t.whole) + 86400);
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toTime(t) + 86400);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToDateImpl<>;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfMinute(t.whole));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfMinute(t.whole), 0};
    }
    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

// Rounding towards negative infinity.
// 1.01 => 1.00
// -1.01 => -2
struct ToStartOfSecondImpl
{
    static constexpr auto name = "toStartOfSecond";

    static DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
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

    static UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMillisecondImpl
{
    static constexpr auto name = "toStartOfMillisecond";

    static DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        // given that scale is 6, scale_multiplier is 1000000
        // for DateTime64 value of 123.456789:
        // 123456789 - 789 = 123456000
        // for DateTime64 value of -123.456789:
        // -123456789 - (1000 + (-789)) = -123457000

        if (scale_multiplier == 1000)
        {
            return datetime64;
        }
        if (scale_multiplier <= 1000)
        {
            return datetime64 * (1000 / scale_multiplier);
        }

        auto droppable_part_with_sign
            = DecimalUtils::getFractionalPartWithScaleMultiplier<DateTime64, true>(datetime64, scale_multiplier / 1000);

        if (droppable_part_with_sign < 0)
            droppable_part_with_sign += scale_multiplier;

        return datetime64 - droppable_part_with_sign;
    }

    static UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMicrosecondImpl
{
    static constexpr auto name = "toStartOfMicrosecond";

    static DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        // @see ToStartOfMillisecondImpl

        if (scale_multiplier == 1000000)
        {
            return datetime64;
        }
        if (scale_multiplier <= 1000000)
        {
            return datetime64 * (1000000 / scale_multiplier);
        }

        auto droppable_part_with_sign
            = DecimalUtils::getFractionalPartWithScaleMultiplier<DateTime64, true>(datetime64, scale_multiplier / 1000000);

        if (droppable_part_with_sign < 0)
            droppable_part_with_sign += scale_multiplier;

        return datetime64 - droppable_part_with_sign;
    }

    static UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfNanosecondImpl
{
    static constexpr auto name = "toStartOfNanosecond";

    static DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        // @see ToStartOfMillisecondImpl
        if (scale_multiplier == 1000000000)
        {
            return datetime64;
        }
        if (scale_multiplier <= 1000000000)
        {
            return datetime64 * (1000000000 / scale_multiplier);
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument for function {}, DateTime64 expected", name);
    }

    static UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinutesImpl
{
    static constexpr auto name = "toStartOfFiveMinutes";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfFiveMinutes(t.whole));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinutes(t);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfFiveMinutes(t.whole), 0};
    }
    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfTenMinutesImpl
{
    static constexpr auto name = "toStartOfTenMinutes";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfTenMinutes(t.whole));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfTenMinutes(t);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfTenMinutes(t.whole), 0};
    }
    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFifteenMinutesImpl
{
    static constexpr auto name = "toStartOfFifteenMinutes";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfFifteenMinutes(t.whole));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t);
    }
    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfFifteenMinutes(t.whole), 0};
    }
    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

/// Round to start of half-an-hour length interval with unspecified offset. This transform is specific for Metrica web analytics system.
struct TimeSlotImpl
{
    static constexpr auto name = "timeSlot";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    {
        return static_cast<UInt32>(t.whole / 1800 * 1800);
    }

    static UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t / 1800 * 1800;
    }

    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }

    static DecimalUtils::DecimalComponents<DateTime64>  executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    {
        if (likely(t.whole >= 0))
            return {t.whole / 1800 * 1800, 0};
        return {(t.whole + 1 - 1800) / 1800 * 1800, 0};
    }

    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfHour(t.whole));
    }

    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t);
    }

    static UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    static UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }

    static DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfHour(t.whole), 0};
    }

    static Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
    static constexpr auto name = "toYear";
    static UInt16 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }

    static constexpr bool hasPreimage() { return true; }

    static OptionalFieldInterval getPreimage(const IDataType & type, const Field & point)
    {
        if (point.getType() != Field::Types::UInt64) return std::nullopt;

        auto year = point.safeGet<UInt64>();
        if (year < DATE_LUT_MIN_YEAR || year >= DATE_LUT_MAX_YEAR) return std::nullopt;

        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        auto start_time = date_lut.makeDateTime(year, 1, 1, 0, 0, 0);
        auto end_time = date_lut.addYears(start_time, 1);

        if (isDateOrDate32(type) || isDateTime(type) || isDateTime64(type))
            return {std::make_pair(Field(start_time), Field(end_time))};
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
            type.getName(),
            name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekYearImpl
{
    static constexpr auto name = "toWeekYear";

    static constexpr Int8 week_mode = 3;
    static UInt16 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(t, week_mode).first;
    }
    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(t, week_mode).first;
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(t, week_mode).first;
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(ExtendedDayNum(d), week_mode).first;
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(DayNum(d), week_mode).first;
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekOfWeekYearImpl
{
    static constexpr auto name = "toWeekOfWeekYear";
    static UInt16 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(t);
    }
    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(t);
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(t);
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToQuarterImpl
{
    static constexpr auto name = "toQuarter";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(ExtendedDayNum(d));
    }
    static UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToMonthImpl
{
    static constexpr auto name = "toMonth";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(ExtendedDayNum(d));
    }
    static UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
    static constexpr auto name = "toDayOfMonth";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(ExtendedDayNum(d));
    }
    static UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(DayNum(d));
    }

    static constexpr bool hasPreimage() { return false; }
    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";
    static constexpr bool value_may_be_string = true;
    static UInt8 execute(UInt64 t, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t, mode);
    }
    static UInt8 execute(Int64 t, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t, mode);
    }
    static UInt8 execute(UInt32 t, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t, mode);
    }
    static UInt8 execute(Int32 d, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(ExtendedDayNum(d), mode);
    }
    static UInt8 execute(UInt16 d, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(DayNum(d), mode);
    }

    using FactorTransform = ToMondayImpl;
};

struct ToDayOfYearImpl
{
    static constexpr auto name = "toDayOfYear";
    static UInt16 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDaysSinceYearZeroImpl
{
private:
    static constexpr auto SECONDS_PER_DAY = 60 * 60 * 24;

public:
    static constexpr auto DAYS_BETWEEN_YEARS_0_AND_1970 = 719'528; /// 01 January, each. Constant taken from Java LocalDate. Consistent with MySQL's TO_DAYS().

    static constexpr auto name = "toDaysSinceYearZero";

    static UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return DAYS_BETWEEN_YEARS_0_AND_1970 + static_cast<UInt32>(time_zone.toDayNum(t));
    }
    static UInt32 execute(UInt32 d, const DateLUTImpl &)
    {
        return DAYS_BETWEEN_YEARS_0_AND_1970 + d / SECONDS_PER_DAY;
    }
    static UInt32 execute(Int32 d, const DateLUTImpl &)
    {
        return DAYS_BETWEEN_YEARS_0_AND_1970 + d;
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl &)
    {
        return DAYS_BETWEEN_YEARS_0_AND_1970 + d;
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToHourImpl
{
    static constexpr auto name = "toHour";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }
    static UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToDateImpl<>;
};

struct TimezoneOffsetImpl
{
    static constexpr auto name = "timezoneOffset";
    static time_t execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.timezoneOffset(t);
    }
    static time_t execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.timezoneOffset(t);
    }

    static time_t execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.timezoneOffset(t);
    }

    static time_t execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    static time_t execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }

    static constexpr bool hasPreimage() { return false; }
    using FactorTransform = ToTimeImpl;
};

struct ToMinuteImpl
{
    static constexpr auto name = "toMinute";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
    static constexpr auto name = "toSecond";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToStartOfMinuteImpl;
};

struct ToMillisecondImpl
{
    static constexpr auto name = "toMillisecond";

    static UInt16 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl & time_zone)
    {
        return time_zone.toMillisecond(datetime64, scale_multiplier);
    }

    static UInt16 execute(UInt32, const DateLUTImpl &)
    {
        return 0;
    }
    static UInt16 execute(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }
    static UInt16 execute(UInt16, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToISOYearImpl
{
    static constexpr auto name = "toISOYear";
    static UInt16 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(ExtendedDayNum(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfISOYearImpl
{
    static constexpr auto name = "toStartOfISOYear";

    static UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return t < 0 ? 0 : time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(std::min(Int32(time_zone.toDayNum(t)), Int32(DATE_LUT_MAX_DAY_NUM))));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return d < 0 ? 0 : time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(std::min(d, Int32(DATE_LUT_MAX_DAY_NUM))));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(DayNum(d));
    }
    static Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToISOWeekImpl
{
    static constexpr auto name = "toISOWeek";
    static UInt8 execute(UInt64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static UInt8 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(ExtendedDayNum(d));
    }
    static UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToISOYearImpl;
};

enum class ResultPrecision : uint8_t
{
    Standard,
    Extended
};

/// Standard precision results (precision_ == ResultPrecision::Standard) potentially lead to overflows when returning values.
/// This mode is used by SQL functions "toRelative*Num()" which cannot easily be changed due to backward compatibility.
/// According to documentation, these functions merely need to compute the time difference to a deterministic, fixed point in the past.
/// As a future TODO, we should fix their behavior in a backwards-compatible way.
/// See https://github.com/ClickHouse/ClickHouse/issues/41977#issuecomment-1267536814.
template <ResultPrecision precision_>
struct ToRelativeYearNumImpl
{
    static constexpr auto name = "toRelativeYearNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toYear(t);
        else
            return static_cast<UInt16>(time_zone.toYear(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(static_cast<time_t>(t));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toYear(ExtendedDayNum(d));
        else
            return static_cast<UInt16>(time_zone.toYear(ExtendedDayNum(d)));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeQuarterNum(t);
        else
            return static_cast<UInt16>(time_zone.toRelativeQuarterNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(static_cast<time_t>(t));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeQuarterNum(ExtendedDayNum(d));
        else
            return static_cast<UInt16>(time_zone.toRelativeQuarterNum(ExtendedDayNum(d)));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeMonthNum(t);
        else
            return static_cast<UInt16>(time_zone.toRelativeMonthNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(static_cast<time_t>(t));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeMonthNum(ExtendedDayNum(d));
        else
            return static_cast<UInt16>(time_zone.toRelativeMonthNum(ExtendedDayNum(d)));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeWeekNum(t);
        else
            return static_cast<UInt16>(time_zone.toRelativeWeekNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(static_cast<time_t>(t));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return time_zone.toRelativeWeekNum(ExtendedDayNum(d));
        else
            return static_cast<UInt16>(time_zone.toRelativeWeekNum(ExtendedDayNum(d)));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toDayNum(t));
        else
            return static_cast<UInt16>(time_zone.toDayNum(t));
    }
    static UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(static_cast<time_t>(t));
    }
    static auto execute(Int32 d, const DateLUTImpl &)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(static_cast<ExtendedDayNum>(d));
        else
            return static_cast<UInt16>(static_cast<ExtendedDayNum>(d));
    }
    static UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return static_cast<DayNum>(d);
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeHourNumImpl
{
    static constexpr auto name = "toRelativeHourNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toStableRelativeHourNum(t));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(t));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<UInt32>(time_zone.toStableRelativeHourNum(static_cast<DateLUTImpl::Time>(t)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(static_cast<DateLUTImpl::Time>(t)));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toStableRelativeHourNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(ExtendedDayNum(d)));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<UInt32>(time_zone.toStableRelativeHourNum(DayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(DayNum(d)));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toRelativeMinuteNum(t));
        else
            return static_cast<UInt32>(time_zone.toRelativeMinuteNum(t));
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toRelativeMinuteNum(static_cast<DateLUTImpl::Time>(t)));
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toRelativeMinuteNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeMinuteNum(ExtendedDayNum(d)));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toRelativeMinuteNum(DayNum(d)));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeSecondNumImpl
{
    static constexpr auto name = "toRelativeSecondNum";

    static Int64 execute(Int64 t, const DateLUTImpl &)
    {
        return t;
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t;
    }
    static auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.fromDayNum(ExtendedDayNum(d)));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <Int64 scale_multiplier>
struct ToRelativeSubsecondNumImpl
{
    static constexpr auto name = "toRelativeSubsecondNumImpl";

    static Int64 execute(const DateTime64 & t, const DateTime64::NativeType scale, const DateLUTImpl &)
    {
        static_assert(
            scale_multiplier == millisecond_multiplier || scale_multiplier == microsecond_multiplier || scale_multiplier == nanosecond_multiplier);
        if (scale == scale_multiplier)
            return t.value;
        if (scale > scale_multiplier)
            return t.value / (scale / scale_multiplier);
        return common::mulIgnoreOverflow(t.value, scale_multiplier / scale);
    }
    static Int64 execute(UInt32 t, const DateLUTImpl &)
    {
        return common::mulIgnoreOverflow(static_cast<Int64>(t), scale_multiplier);
    }
    static Int64 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return common::mulIgnoreOverflow(static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(d))), scale_multiplier);
    }
    static Int64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return common::mulIgnoreOverflow(static_cast<Int64>(time_zone.fromDayNum(DayNum(d))), scale_multiplier);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMImpl
{
    static constexpr auto name = "toYYYYMM";

    static UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(t);
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(t);
    }
    static UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(ExtendedDayNum(d));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(DayNum(d));
    }
    static constexpr bool hasPreimage() { return true; }

    static OptionalFieldInterval getPreimage(const IDataType & type, const Field & point)
    {
        if (point.getType() != Field::Types::UInt64) return std::nullopt;

        auto year_month = point.safeGet<UInt64>();
        auto year = year_month / 100;
        auto month = year_month % 100;

        if (year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || (year == DATE_LUT_MAX_YEAR && month == 12))
            return std::nullopt;

        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        auto start_time = date_lut.makeDateTime(year, month, 1, 0, 0, 0);
        auto end_time = date_lut.addMonths(start_time, 1);

        if (isDateOrDate32(type) || isDateTime(type) || isDateTime64(type))
            return {std::make_pair(Field(start_time), Field(end_time))};
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
            type.getName(),
            name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDImpl
{
    static constexpr auto name = "toYYYYMMDD";

    static UInt32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(t);
    }
    static UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(t);
    }
    static UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(ExtendedDayNum(d));
    }
    static UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(DayNum(d));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMDDhhmmssImpl
{
    static constexpr auto name = "toYYYYMMDDhhmmss";

    static UInt64 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(t);
    }
    static UInt64 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(t);
    }
    static UInt64 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(ExtendedDayNum(d)));
    }
    static UInt64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(DayNum(d)));
    }
    static constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct DateTimeComponentsWithFractionalPart : public DateLUTImpl::DateTimeComponents
{
    UInt16  millisecond;
    UInt16  microsecond;
    UInt16  nanosecond;
};

struct ToDateTimeComponentsImpl
{
    static constexpr auto name = "toDateTimeComponents";

    static DateTimeComponentsWithFractionalPart execute(const DateTime64 & t, const DateTime64::NativeType scale_multiplier, const DateLUTImpl & time_zone)
    {
        auto components = DecimalUtils::splitWithScaleMultiplier(t, scale_multiplier);

        if (t.value < 0 && components.fractional)
        {
            components.fractional = scale_multiplier + (components.whole ? Int64(-1) : Int64(1)) * components.fractional;
            --components.whole;
        }

        // Normalize the dividers between microseconds and nanoseconds w.r.t. the scale.
        Int64 microsecond_divider = (millisecond_multiplier * scale_multiplier) / microsecond_multiplier;
        Int64 nanosecond_divider = scale_multiplier / microsecond_multiplier;

        // Protect against division by zero for smaller scale multipliers.
        microsecond_divider = (microsecond_divider ? microsecond_divider : 1);
        nanosecond_divider = (nanosecond_divider ? nanosecond_divider : 1);

        const Int64 & fractional = components.fractional;
        UInt16 millisecond = static_cast<UInt16>(fractional / microsecond_divider);
        UInt16 microsecond = static_cast<UInt16>((fractional % microsecond_divider) / nanosecond_divider);
        UInt16 nanosecond = static_cast<UInt16>(fractional % nanosecond_divider);

        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(components.whole), millisecond, microsecond, nanosecond};
    }
    static DateTimeComponentsWithFractionalPart execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(static_cast<DateLUTImpl::Time>(t)), 0, 0, 0};
    }
    static DateTimeComponentsWithFractionalPart execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(ExtendedDayNum(d)), 0, 0, 0};
    }
    static DateTimeComponentsWithFractionalPart execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(DayNum(d)), 0, 0, 0};
    }

    using FactorTransform = ZeroTransform;
};

struct DateTimeAccurateConvertStrategyAdditions {};
struct DateTimeAccurateOrNullConvertStrategyAdditions {};

template <typename FromType, typename ToType, typename Transform, bool is_extended_result = false, typename Additions = void *>
struct Transformer
{
    template <typename FromTypeVector, typename ToTypeVector>
    static void vector(const FromTypeVector & vec_from, ToTypeVector & vec_to, const DateLUTImpl & time_zone, const Transform & transform,
        [[maybe_unused]] ColumnUInt8::Container * vec_null_map_to, size_t input_rows_count)
    {
        using ValueType = typename ToTypeVector::value_type;
        vec_to.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if constexpr (std::is_same_v<ToType, DataTypeDate> || std::is_same_v<ToType, DataTypeDateTime>)
            {
                if constexpr (std::is_same_v<Additions, DateTimeAccurateConvertStrategyAdditions>
                    || std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
                {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wimplicit-const-int-float-conversion"
                    bool is_valid_input = vec_from[i] >= 0 && vec_from[i] <= 0xFFFFFFFFL;
#pragma clang diagnostic pop
                    if (!is_valid_input)
                    {
                        if constexpr (std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
                        {
                            vec_to[i] = 0;
                            (*vec_null_map_to)[i] = true;
                            continue;
                        }
                        else
                        {
                            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Value {} cannot be safely converted into type {}",
                                vec_from[i], TypeName<ValueType>);
                        }
                    }
                }
            }

            if constexpr (is_extended_result)
                vec_to[i] = static_cast<ValueType>(transform.executeExtendedResult(vec_from[i], time_zone));
            else
                vec_to[i] = static_cast<ValueType>(transform.execute(vec_from[i], time_zone));
        }
    }
};

template <typename FromDataType, typename ToDataType, typename Transform, bool is_extended_result = false>
struct DateTimeTransformImpl
{
    template <typename Additions = void *>
    static ColumnPtr execute(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, const Transform & transform = {})
    {
        using Op = Transformer<FromDataType, ToDataType, Transform, is_extended_result, Additions>;

        const ColumnPtr source_col = arguments[0].column;
        if (const auto * sources = checkAndGetColumn<typename FromDataType::ColumnType>(source_col.get()))
        {
            ColumnUInt8::MutablePtr col_null_map_to;
            ColumnUInt8::Container * vec_null_map_to [[maybe_unused]] = nullptr;
            if constexpr (std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
            {
                col_null_map_to = ColumnUInt8::create(sources->getData().size(), false);
                vec_null_map_to = &col_null_map_to->getData();
            }

            auto mutable_result_col = result_type->createColumn();
            auto * col_to = assert_cast<typename ToDataType::ColumnType *>(mutable_result_col.get());

            WhichDataType result_data_type(result_type);
            if (result_data_type.isDateTime() || result_data_type.isDateTime64())
            {
                const auto & time_zone = dynamic_cast<const TimezoneMixin &>(*result_type).getTimeZone();
                Op::vector(sources->getData(), col_to->getData(), time_zone, transform, vec_null_map_to, input_rows_count);
            }
            else
            {
                size_t time_zone_argument_position = 1;
                if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                    time_zone_argument_position = 2;

                const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, time_zone_argument_position, 0);
                Op::vector(sources->getData(), col_to->getData(), time_zone, transform, vec_null_map_to, input_rows_count);
            }

            if constexpr (std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
            {
                if (vec_null_map_to)
                    return ColumnNullable::create(std::move(mutable_result_col), std::move(col_null_map_to));
            }

            return mutable_result_col;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of first argument of function {}",
            arguments[0].column->getName(),
            Transform::name);
    }
};

}
