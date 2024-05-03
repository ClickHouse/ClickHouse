#pragma once

#include <base/types.h>
#include <Core/DecimalFunctions.h>
#include <Common/Exception.h>
#include <Common/DateLUTImpl.h>
#include <Common/DateLUT.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{

static constexpr auto microsecond_multiplier = 1000000;
static constexpr auto millisecond_multiplier = 1000;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_CONVERT_TYPE;
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

[[noreturn]] void throwDateIsNotSupported(const char * name);
[[noreturn]] void throwDateTimeIsNotSupported(const char * name);
[[noreturn]] void throwDate32IsNotSupported(const char * name);

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(Int64, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt32, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(Int32, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, const DateLUTImpl &) { return 0; }
};

struct ToDateImpl
{
    static constexpr auto name = "toDate";

    static inline UInt16 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt16>(time_zone.toDayNum(t.whole));
    }
    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return UInt16(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(Int32, const DateLUTImpl &)
    {
        throwDateIsNotSupported(name);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toDayNum(t.whole), 0};
    }

    using FactorTransform = ZeroTransform;
};

struct ToDate32Impl
{
    static constexpr auto name = "toDate32";

    static inline Int32 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return Int32(time_zone.toDayNum(t));
    }
    static inline Int32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        /// Don't saturate.
        return Int32(time_zone.toDayNum<Int64>(t));
    }
    static inline Int32 execute(Int32 d, const DateLUTImpl &)
    {
        return d;
    }
    static inline Int32 execute(UInt16 d, const DateLUTImpl &)
    {
        return d;
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfDayImpl
{
    static constexpr auto name = "toStartOfDay";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(static_cast<time_t>(t.whole)));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(t));
    }
    static inline UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(ExtendedDayNum(d)));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toDate(DayNum(d)));
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toDate(t.whole), 0};
    }
    static inline Int64 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(ExtendedDayNum(d)) * DecimalUtils::scaleMultiplier<DateTime64>(DataTypeDateTime64::default_scale);
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToLastDayOfMonthImpl
{
    static constexpr auto name = "toLastDayOfMonth";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toLastDayNumOfMonth(ExtendedDayNum(d));
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(ExtendedDayNum(d));
    }

    using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
    /// When transforming to time, the date will be equated to 1970-01-02.
    static constexpr auto name = "toTime";

    static UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toTime(t.whole) + 86400);
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toTime(t) + 86400);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfMinute(t.whole));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfMinute(t.whole), 0};
    }
    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
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
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMillisecondImpl
{
    static constexpr auto name = "toStartOfMillisecond";

    static inline DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
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
        else if (scale_multiplier <= 1000)
        {
            return datetime64 * (1000 / scale_multiplier);
        }
        else
        {
        auto droppable_part_with_sign = DecimalUtils::getFractionalPartWithScaleMultiplier<DateTime64, true>(datetime64, scale_multiplier / 1000);

        if (droppable_part_with_sign < 0)
            droppable_part_with_sign += scale_multiplier;

        return datetime64 - droppable_part_with_sign;
        }
    }

    static inline UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMicrosecondImpl
{
    static constexpr auto name = "toStartOfMicrosecond";

    static inline DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        // @see ToStartOfMillisecondImpl

        if (scale_multiplier == 1000000)
        {
            return datetime64;
        }
        else if (scale_multiplier <= 1000000)
        {
            return datetime64 * (1000000 / scale_multiplier);
        }
        else
        {
            auto droppable_part_with_sign = DecimalUtils::getFractionalPartWithScaleMultiplier<DateTime64, true>(datetime64, scale_multiplier / 1000000);

            if (droppable_part_with_sign < 0)
                droppable_part_with_sign += scale_multiplier;

            return datetime64 - droppable_part_with_sign;
        }
    }

    static inline UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfNanosecondImpl
{
    static constexpr auto name = "toStartOfNanosecond";

    static inline DateTime64 execute(const DateTime64 & datetime64, Int64 scale_multiplier, const DateLUTImpl &)
    {
        // @see ToStartOfMillisecondImpl
        if (scale_multiplier == 1000000000)
        {
            return datetime64;
        }
        else if (scale_multiplier <= 1000000000)
        {
            return datetime64 * (1000000000 / scale_multiplier);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of argument for function {}, DateTime64 expected", name);
        }
    }

    static inline UInt32 execute(UInt32, const DateLUTImpl &)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type DateTime of argument for function {}", name);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinutesImpl
{
    static constexpr auto name = "toStartOfFiveMinutes";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfFiveMinutes(t.whole));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinutes(t);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfFiveMinutes(t.whole), 0};
    }
    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfTenMinutesImpl
{
    static constexpr auto name = "toStartOfTenMinutes";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfTenMinutes(t.whole));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfTenMinutes(t);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfTenMinutes(t.whole), 0};
    }
    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFifteenMinutesImpl
{
    static constexpr auto name = "toStartOfFifteenMinutes";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfFifteenMinutes(t.whole));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t);
    }
    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfFifteenMinutes(t.whole), 0};
    }
    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

/// Round to start of half-an-hour length interval with unspecified offset. This transform is specific for Metrica web analytics system.
struct TimeSlotImpl
{
    static constexpr auto name = "timeSlot";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    {
        return static_cast<UInt32>(t.whole / 1800 * 1800);
    }

    static inline UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t / 1800 * 1800;
    }

    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline DecimalUtils::DecimalComponents<DateTime64>  executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl &)
    {
        if (likely(t.whole >= 0))
            return {t.whole / 1800 * 1800, 0};
        return {(t.whole + 1 - 1800) / 1800 * 1800, 0};
    }

    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static inline UInt32 execute(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toStartOfHour(t.whole));
    }

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t);
    }

    static inline UInt32 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline DecimalUtils::DecimalComponents<DateTime64> executeExtendedResult(const DecimalUtils::DecimalComponents<DateTime64> & t, const DateLUTImpl & time_zone)
    {
        return {time_zone.toStartOfHour(t.whole), 0};
    }

    static inline Int64 executeExtendedResult(Int32, const DateLUTImpl &)
    {
        throwDate32IsNotSupported(name);
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }

    static inline constexpr bool hasPreimage() { return true; }

    static inline RangeOrNull getPreimage(const IDataType & type, const Field & point)
    {
        if (point.getType() != Field::Types::UInt64) return std::nullopt;

        auto year = point.get<UInt64>();
        if (year < DATE_LUT_MIN_YEAR || year >= DATE_LUT_MAX_YEAR) return std::nullopt;

        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        auto start_time = date_lut.makeDateTime(year, 1, 1, 0, 0, 0);
        auto end_time = date_lut.addYears(start_time, 1);

        if (isDateOrDate32(type) || isDateTime(type) || isDateTime64(type))
            return {std::make_pair(Field(start_time), Field(end_time))};
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
                type.getName(), name);
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekYearImpl
{
    static constexpr auto name = "toWeekYear";

    static constexpr Int8 week_mode = 3;

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(t, week_mode).first;
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(t, week_mode).first;
    }
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(ExtendedDayNum(d), week_mode).first;
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYearWeek(DayNum(d), week_mode).first;
    }

    using FactorTransform = ZeroTransform;
};

struct ToWeekOfWeekYearImpl
{
    static constexpr auto name = "toWeekOfWeekYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(t);
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(t);
    }
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(DayNum(d));
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
    static inline UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(ExtendedDayNum(d));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(ExtendedDayNum(d));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(ExtendedDayNum(d));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(DayNum(d));
    }

    static inline constexpr bool hasPreimage() { return false; }
    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";

    static inline UInt8 execute(Int64 t, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t, mode);
    }
    static inline UInt8 execute(UInt32 t, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t, mode);
    }
    static inline UInt8 execute(Int32 d, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(ExtendedDayNum(d), mode);
    }
    static inline UInt8 execute(UInt16 d, UInt8 mode, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(DayNum(d), mode);
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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

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

    static inline time_t execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline time_t execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }

    static inline constexpr bool hasPreimage() { return false; }
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
    static inline UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt8 execute(Int32, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throwDateTimeIsNotSupported(name);
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(ExtendedDayNum(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfISOYearImpl
{
    static constexpr auto name = "toStartOfISOYear";

    static inline UInt16 execute(Int64 t, const DateLUTImpl & time_zone)
    {
        return t < 0 ? 0 : time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(std::min(Int32(time_zone.toDayNum(t)), Int32(DATE_LUT_MAX_DAY_NUM))));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return d < 0 ? 0 : time_zone.toFirstDayNumOfISOYear(ExtendedDayNum(std::min(d, Int32(DATE_LUT_MAX_DAY_NUM))));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(DayNum(d));
    }
    static inline Int64 executeExtendedResult(Int64 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static inline Int32 executeExtendedResult(Int32 d, const DateLUTImpl & time_zone)
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
    static inline UInt8 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(ExtendedDayNum(d));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ToISOYearImpl;
};

enum class ResultPrecision
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

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int16>(time_zone.toYear(t));
        else
            return static_cast<UInt16>(time_zone.toYear(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(static_cast<time_t>(t));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int16>(time_zone.toYear(ExtendedDayNum(d)));
        else
            return static_cast<UInt16>(time_zone.toYear(ExtendedDayNum(d)));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeQuarterNum(t));
        else
            return static_cast<UInt16>(time_zone.toRelativeQuarterNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(static_cast<time_t>(t));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeQuarterNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt16>(time_zone.toRelativeQuarterNum(ExtendedDayNum(d)));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeMonthNum(t));
        else
            return static_cast<UInt16>(time_zone.toRelativeMonthNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(static_cast<time_t>(t));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeMonthNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt16>(time_zone.toRelativeMonthNum(ExtendedDayNum(d)));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeWeekNum(t));
        else
            return static_cast<UInt16>(time_zone.toRelativeWeekNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(static_cast<time_t>(t));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(time_zone.toRelativeWeekNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt16>(time_zone.toRelativeWeekNum(ExtendedDayNum(d)));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toDayNum(t));
        else
            return static_cast<UInt16>(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(static_cast<time_t>(t));
    }
    static inline auto execute(Int32 d, const DateLUTImpl &)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int32>(static_cast<ExtendedDayNum>(d));
        else
            return static_cast<UInt16>(static_cast<ExtendedDayNum>(d));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl &)
    {
        return static_cast<DayNum>(d);
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeHourNumImpl
{
    static constexpr auto name = "toRelativeHourNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toStableRelativeHourNum(t));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(t));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<UInt32>(time_zone.toStableRelativeHourNum(static_cast<DateLUTImpl::Time>(t)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(static_cast<DateLUTImpl::Time>(t)));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toStableRelativeHourNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(ExtendedDayNum(d)));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<UInt32>(time_zone.toStableRelativeHourNum(DayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeHourNum(DayNum(d)));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static inline auto execute(Int64 t, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toRelativeMinuteNum(t));
        else
            return static_cast<UInt32>(time_zone.toRelativeMinuteNum(t));
    }
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toRelativeMinuteNum(static_cast<DateLUTImpl::Time>(t)));
    }
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.toRelativeMinuteNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.toRelativeMinuteNum(ExtendedDayNum(d)));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.toRelativeMinuteNum(DayNum(d)));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <ResultPrecision precision_>
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
    static inline auto execute(Int32 d, const DateLUTImpl & time_zone)
    {
        if constexpr (precision_ == ResultPrecision::Extended)
            return static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(d)));
        else
            return static_cast<UInt32>(time_zone.fromDayNum(ExtendedDayNum(d)));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<UInt32>(time_zone.fromDayNum(DayNum(d)));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

template <Int64 scale_multiplier>
struct ToRelativeSubsecondNumImpl
{
    static constexpr auto name = "toRelativeSubsecondNumImpl";

    static inline Int64 execute(const DateTime64 & t, DateTime64::NativeType scale, const DateLUTImpl &)
    {
        static_assert(scale_multiplier == 1000 || scale_multiplier == 1000000);
        if (scale == scale_multiplier)
            return t.value;
        if (scale > scale_multiplier)
            return t.value / (scale / scale_multiplier);
        return t.value * (scale_multiplier / scale);
    }
    static inline Int64 execute(UInt32 t, const DateLUTImpl &)
    {
        return t * scale_multiplier;
    }
    static inline Int64 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return static_cast<Int64>(time_zone.fromDayNum(ExtendedDayNum(d))) * scale_multiplier;
    }
    static inline Int64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return static_cast<Int64>(time_zone.fromDayNum(DayNum(d)) * scale_multiplier);
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
    static inline UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(ExtendedDayNum(d));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMM(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return true; }

    static inline RangeOrNull getPreimage(const IDataType & type, const Field & point)
    {
        if (point.getType() != Field::Types::UInt64) return std::nullopt;

        auto year_month = point.get<UInt64>();
        auto year = year_month / 100;
        auto month = year_month % 100;

        if (year < DATE_LUT_MIN_YEAR || year > DATE_LUT_MAX_YEAR || month < 1 || month > 12 || (year == DATE_LUT_MAX_YEAR && month == 12))
            return std::nullopt;

        const DateLUTImpl & date_lut = DateLUT::instance("UTC");

        auto start_time = date_lut.makeDateTime(year, month, 1, 0, 0, 0);
        auto end_time = date_lut.addMonths(start_time, 1);

        if (isDateOrDate32(type) || isDateTime(type) || isDateTime64(type))
            return {std::make_pair(Field(start_time), Field(end_time))};
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Should be Date, Date32, DateTime or DateTime64",
                type.getName(), name);
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
    static inline UInt32 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(ExtendedDayNum(d));
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDD(DayNum(d));
    }
    static inline constexpr bool hasPreimage() { return false; }

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
    static inline UInt64 execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(ExtendedDayNum(d)));
    }
    static inline UInt64 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toNumYYYYMMDDhhmmss(time_zone.toDate(DayNum(d)));
    }
    static inline constexpr bool hasPreimage() { return false; }

    using FactorTransform = ZeroTransform;
};

struct DateTimeComponentsWithFractionalPart : public DateLUTImpl::DateTimeComponents
{
    UInt16  millisecond;
    UInt16  microsecond;
};

struct ToDateTimeComponentsImpl
{
    static constexpr auto name = "toDateTimeComponents";

    static inline DateTimeComponentsWithFractionalPart execute(const DateTime64 & t, DateTime64::NativeType scale_multiplier, const DateLUTImpl & time_zone)
    {
        auto components = DecimalUtils::splitWithScaleMultiplier(t, scale_multiplier);

        if (t.value < 0 && components.fractional)
        {
            components.fractional = scale_multiplier + (components.whole ? Int64(-1) : Int64(1)) * components.fractional;
            --components.whole;
        }
        Int64 fractional = components.fractional;
        if (scale_multiplier > microsecond_multiplier)
            fractional = fractional / (scale_multiplier / microsecond_multiplier);
        else if (scale_multiplier < microsecond_multiplier)
            fractional = fractional * (microsecond_multiplier / scale_multiplier);

        constexpr Int64 divider = microsecond_multiplier/ millisecond_multiplier;
        UInt16 millisecond = static_cast<UInt16>(fractional / divider);
        UInt16 microsecond = static_cast<UInt16>(fractional % divider);
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(components.whole), millisecond, microsecond};
    }
    static inline DateTimeComponentsWithFractionalPart execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(static_cast<DateLUTImpl::Time>(t)), 0, 0};
    }
    static inline DateTimeComponentsWithFractionalPart execute(Int32 d, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(ExtendedDayNum(d)), 0, 0};
    }
    static inline DateTimeComponentsWithFractionalPart execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return DateTimeComponentsWithFractionalPart{time_zone.toDateTimeComponents(DayNum(d)), 0, 0};
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
        [[maybe_unused]] ColumnUInt8::Container * vec_null_map_to)
    {
        using ValueType = typename ToTypeVector::value_type;
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (std::is_same_v<ToType, DataTypeDate> || std::is_same_v<ToType, DataTypeDateTime>)
            {
                if constexpr (std::is_same_v<Additions, DateTimeAccurateConvertStrategyAdditions>
                    || std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
                {
                    bool is_valid_input = vec_from[i] >= 0 && vec_from[i] <= 0xFFFFFFFFL;

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
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/, const Transform & transform = {})
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
                Op::vector(sources->getData(), col_to->getData(), time_zone, transform, vec_null_map_to);
            }
            else
            {
                size_t time_zone_argument_position = 1;
                if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
                    time_zone_argument_position = 2;

                const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, time_zone_argument_position, 0);
                Op::vector(sources->getData(), col_to->getData(), time_zone, transform, vec_null_map_to);
            }

            if constexpr (std::is_same_v<Additions, DateTimeAccurateOrNullConvertStrategyAdditions>)
            {
                if (vec_null_map_to)
                {
                    return ColumnNullable::create(std::move(mutable_result_col), std::move(col_null_map_to));
                }
            }

            return mutable_result_col;
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), Transform::name);
        }
    }
};

}
