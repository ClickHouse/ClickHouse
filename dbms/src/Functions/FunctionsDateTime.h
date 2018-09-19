#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

#include <Common/typeid_cast.h>

#include <IO/WriteHelpers.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>

#include <common/DateLUT.h>

#include <Poco/String.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Functions for working with date and time.
  *
  * toYear, toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond,
  * toMonday, toStartOfMonth, toStartOfYear, toStartOfMinute, toStartOfFiveMinute, toStartOfFifteenMinutes
  * toStartOfHour, toTime,
  * now, today, yesterday
  * TODO: makeDate, makeDateTime
  *
  * (toDate - located in FunctionConversion.h file)
  *
  * Return types:
  *  toYear -> UInt16
  *  toMonth, toDayOfMonth, toDayOfWeek, toHour, toMinute, toSecond -> UInt8
  *  toMonday, toStartOfMonth, toStartOfYear -> Date
  *  toStartOfMinute, toStartOfHour, toTime, now -> DateTime
  *
  * And also:
  *
  * timeSlot(EventTime)
  * - rounds the time to half an hour.
  *
  * timeSlots(StartTime, Duration)
  * - for the time interval beginning at `StartTime` and continuing `Duration` seconds,
  *   returns an array of time points, consisting of rounding down to half an hour of points from this interval.
  *  For example, timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')].
  *  This is necessary to search for hits that are part of the corresponding visit.
  */


/// Determine working timezone either from optional argument with time zone name or from time zone in DateTime type of argument.
std::string extractTimeZoneNameFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);
const DateLUTImpl & extractTimeZoneFromFunctionArguments(Block & block, const ColumnNumbers & arguments, size_t time_zone_arg_num, size_t datetime_arg_num);



#define TIME_SLOT_SIZE 1800

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

/// This factor transformation will say that the function is monotone everywhere.
struct ZeroTransform
{
    static inline UInt16 execute(UInt32, const DateLUTImpl &) { return 0; }
    static inline UInt16 execute(UInt16, const DateLUTImpl &) { return 0; }
};

struct ToDateImpl
{
    static constexpr auto name = "toDate";

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

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDate(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfDay", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToMondayImpl
{
    static constexpr auto name = "toMonday";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfWeek(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfMonthImpl
{
    static constexpr auto name = "toStartOfMonth";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfMonth(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfQuarterImpl
{
    static constexpr auto name = "toStartOfQuarter";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfQuarter(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfYearImpl
{
    static constexpr auto name = "toStartOfYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfYear(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};


struct ToTimeImpl
{
    static constexpr auto name = "toTime";

    /// When transforming to time, the date will be equated to 1970-01-02.
    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toTime(t) + 86400;
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToStartOfMinuteImpl
{
    static constexpr auto name = "toStartOfMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFiveMinuteImpl
{
    static constexpr auto name = "toStartOfFiveMinute";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFiveMinute(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfFiveMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfFifteenMinutesImpl
{
    static constexpr auto name = "toStartOfFifteenMinutes";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfFifteenMinutes(t);
    }
    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfFifteenMinutes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfHourImpl
{
    static constexpr auto name = "toStartOfHour";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toStartOfHour(t);
    }

    static inline UInt32 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toStartOfHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ZeroTransform;
};

struct ToYearImpl
{
    static constexpr auto name = "toYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToQuarterImpl
{
    static constexpr auto name = "toQuarter";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toQuarter(DayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToMonthImpl
{
    static constexpr auto name = "toMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toMonth(DayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToDayOfMonthImpl
{
    static constexpr auto name = "toDayOfMonth";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfMonth(DayNum(d));
    }

    using FactorTransform = ToStartOfMonthImpl;
};

struct ToDayOfWeekImpl
{
    static constexpr auto name = "toDayOfWeek";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(t);
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfWeek(DayNum(d));
    }

    using FactorTransform = ToMondayImpl;
};

struct ToDayOfYearImpl
{
    static constexpr auto name = "toDayOfYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayOfYear(DayNum(d));
    }

    using FactorTransform = ToStartOfYearImpl;
};

struct ToHourImpl
{
    static constexpr auto name = "toHour";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toHour(t);
    }

    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toHour", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToDateImpl;
};

struct ToMinuteImpl
{
    static constexpr auto name = "toMinute";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toMinute(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toMinute", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfHourImpl;
};

struct ToSecondImpl
{
    static constexpr auto name = "toSecond";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toSecond(t);
    }
    static inline UInt8 execute(UInt16, const DateLUTImpl &)
    {
        throw Exception("Illegal type Date of argument for function toSecond", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    using FactorTransform = ToStartOfMinuteImpl;
};

struct ToISOYearImpl
{
    static constexpr auto name = "toISOYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOYear(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToStartOfISOYearImpl
{
    static constexpr auto name = "toStartOfISOYear";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(time_zone.toDayNum(t));
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toFirstDayNumOfISOYear(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToISOWeekImpl
{
    static constexpr auto name = "toISOWeek";

    static inline UInt8 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(time_zone.toDayNum(t));
    }
    static inline UInt8 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toISOWeek(DayNum(d));
    }

    using FactorTransform = ToISOYearImpl;
};

struct ToRelativeYearNumImpl
{
    static constexpr auto name = "toRelativeYearNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toYear(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeQuarterNumImpl
{
    static constexpr auto name = "toRelativeQuarterNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeQuarterNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMonthNumImpl
{
    static constexpr auto name = "toRelativeMonthNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMonthNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeWeekNumImpl
{
    static constexpr auto name = "toRelativeWeekNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(t);
    }
    static inline UInt16 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeWeekNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeDayNumImpl
{
    static constexpr auto name = "toRelativeDayNum";

    static inline UInt16 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toDayNum(t);
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

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeHourNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeMinuteNumImpl
{
    static constexpr auto name = "toRelativeMinuteNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(t);
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.toRelativeMinuteNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToRelativeSecondNumImpl
{
    static constexpr auto name = "toRelativeSecondNum";

    static inline UInt32 execute(UInt32 t, const DateLUTImpl &)
    {
        return t;
    }
    static inline UInt32 execute(UInt16 d, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d));
    }

    using FactorTransform = ZeroTransform;
};

struct ToYYYYMMImpl
{
    static constexpr auto name = "toYYYYMM";

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
    static void vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], time_zone);
    }
};


template <typename FromType, typename ToType, typename Transform>
struct DateTimeTransformImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
    {
        using Op = Transformer<FromType, ToType, Transform>;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 1, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;
        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::vector(sources->getData(), col_to->getData(), time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 1)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!WhichDataType(arguments[0].type).isDateTime()
                || !WhichDataType(arguments[1].type).isString())
                throw Exception(
                    "Function " + getName() + " supports 1 or 2 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// For DateTime, if time zone is specified, attach it to type.
        if (std::is_same_v<ToDataType, DataTypeDateTime>)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
        else
            return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result, input_rows_count);
        else if (which.isDateTime())
            DateTimeTransformImpl<DataTypeDateTime::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result, input_rows_count);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }


    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        IFunction::Monotonicity is_monotonic { true };
        IFunction::Monotonicity is_not_monotonic;

        if (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
        {
            is_monotonic.is_always_monotonic = true;
            return is_monotonic;
        }

        /// This method is called only if the function has one argument. Therefore, we do not care about the non-local time zone.
        const DateLUTImpl & date_lut = DateLUT::instance();

        if (left.isNull() || right.isNull())
            return is_not_monotonic;

        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.

        if (checkAndGetDataType<DataTypeDate>(&type))
        {
            return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), date_lut)
                == Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), date_lut)
                ? is_monotonic : is_not_monotonic;
        }
        else
        {
            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), date_lut)
                == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), date_lut)
                ? is_monotonic : is_not_monotonic;
        }
    }
};


struct AddSecondsImpl
{
    static constexpr auto name = "addSeconds";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta;
    }
};

struct AddMinutesImpl
{
    static constexpr auto name = "addMinutes";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 60;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 60;
    }
};

struct AddHoursImpl
{
    static constexpr auto name = "addHours";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl &)
    {
        return t + delta * 3600;
    }

    static inline UInt32 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.fromDayNum(DayNum(d)) + delta * 3600;
    }
};

struct AddDaysImpl
{
    static constexpr auto name = "addDays";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addDays(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta;
    }
};

struct AddWeeksImpl
{
    static constexpr auto name = "addWeeks";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addWeeks(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl &)
    {
        return d + delta * 7;
    }
};

struct AddMonthsImpl
{
    static constexpr auto name = "addMonths";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addMonths(DayNum(d), delta);
    }
};

struct AddYearsImpl
{
    static constexpr auto name = "addYears";

    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(t, delta);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return time_zone.addYears(DayNum(d), delta);
    }
};


template <typename Transform>
struct SubtractIntervalImpl
{
    static inline UInt32 execute(UInt32 t, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(t, -delta, time_zone);
    }

    static inline UInt16 execute(UInt16 d, Int64 delta, const DateLUTImpl & time_zone)
    {
        return Transform::execute(d, -delta, time_zone);
    }
};

struct SubtractSecondsImpl : SubtractIntervalImpl<AddSecondsImpl> { static constexpr auto name = "subtractSeconds"; };
struct SubtractMinutesImpl : SubtractIntervalImpl<AddMinutesImpl> { static constexpr auto name = "subtractMinutes"; };
struct SubtractHoursImpl : SubtractIntervalImpl<AddHoursImpl> { static constexpr auto name = "subtractHours"; };
struct SubtractDaysImpl : SubtractIntervalImpl<AddDaysImpl> { static constexpr auto name = "subtractDays"; };
struct SubtractWeeksImpl : SubtractIntervalImpl<AddWeeksImpl> { static constexpr auto name = "subtractWeeks"; };
struct SubtractMonthsImpl : SubtractIntervalImpl<AddMonthsImpl> { static constexpr auto name = "subtractMonths"; };
struct SubtractYearsImpl : SubtractIntervalImpl<AddYearsImpl> { static constexpr auto name = "subtractYears"; };


template <typename FromType, typename ToType, typename Transform>
struct Adder
{
    static void vector_vector(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta.getInt(i), time_zone);
    }

    static void vector_constant(const PaddedPODArray<FromType> & vec_from, PaddedPODArray<ToType> & vec_to, Int64 delta, const DateLUTImpl & time_zone)
    {
        size_t size = vec_from.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(vec_from[i], delta, time_zone);
    }

    static void constant_vector(const FromType & from, PaddedPODArray<ToType> & vec_to, const IColumn & delta, const DateLUTImpl & time_zone)
    {
        size_t size = delta.size();
        vec_to.resize(size);

        for (size_t i = 0; i < size; ++i)
            vec_to[i] = Transform::execute(from, delta.getInt(i), time_zone);
    }
};


template <typename FromType, typename Transform>
struct DateTimeAddIntervalImpl
{
    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        using ToType = decltype(Transform::execute(FromType(), 0, std::declval<DateLUTImpl>()));
        using Op = Adder<FromType, ToType, Transform>;

        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);

        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        if (const auto * sources = checkAndGetColumn<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const IColumn & delta_column = *block.getByPosition(arguments[1]).column;

            if (const auto * delta_const_column = typeid_cast<const ColumnConst *>(&delta_column))
                Op::vector_constant(sources->getData(), col_to->getData(), delta_const_column->getField().get<Int64>(), time_zone);
            else
                Op::vector_vector(sources->getData(), col_to->getData(), delta_column, time_zone);

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (const auto * sources = checkAndGetColumnConst<ColumnVector<FromType>>(source_col.get()))
        {
            auto col_to = ColumnVector<ToType>::create();
            Op::constant_vector(sources->template getValue<FromType>(), col_to->getData(), *block.getByPosition(arguments[1]).column, time_zone);
            block.getByPosition(result).column = std::move(col_to);
        }
        else
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                + " of first argument of function " + Transform::name,
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};


template <typename Transform>
class FunctionDateOrDateTimeAddInterval : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateOrDateTimeAddInterval>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNumber(arguments[1].type))
            throw Exception("Second argument for function " + getName() + " (delta) must be number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                    ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }
        else
        {
            if (!WhichDataType(arguments[0].type).isDateTime()
                || !WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Function " + getName() + " supports 2 or 3 arguments. The 1st argument "
                    "must be of type Date or DateTime. The 2nd argument must be number. "
                    "The 3rd argument (optional) must be "
                    "a constant string with timezone name. The timezone argument is allowed "
                    "only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (WhichDataType(arguments[0].type).isDate())
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDate::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
        else
        {
            if (std::is_same_v<decltype(Transform::execute(DataTypeDateTime::FieldType(), 0, std::declval<DateLUTImpl>())), UInt16>)
                return std::make_shared<DataTypeDate>();
            else
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IDataType * from_type = block.getByPosition(arguments[0]).type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            DateTimeAddIntervalImpl<DataTypeDate::FieldType, Transform>::execute(block, arguments, result);
        else if (which.isDateTime())
            DateTimeAddIntervalImpl<DataTypeDateTime::FieldType, Transform>::execute(block, arguments, result);
        else
            throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};


/** dateDiff('unit', t1, t2, [timezone])
  * t1 and t2 can be Date or DateTime
  *
  * If timezone is specified, it applied to both arguments.
  * If not, timezones from datatypes t1 and t2 are used.
  * If that timezones are not the same, the result is unspecified.
  *
  * Timezone matters because days can have different length.
  */
class FunctionDateDiff : public IFunction
{
public:
    static constexpr auto name = "dateDiff";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDateDiff>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 3 or 4",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isString(arguments[0]))
            throw Exception("First argument for function " + getName() + " (unit) must be String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isDateOrDateTime(arguments[1]))
            throw Exception("Second argument for function " + getName() + " must be Date or DateTime",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isDateOrDateTime(arguments[2]))
            throw Exception("Third argument for function " + getName() + " must be Date or DateTime",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 4 && !isString(arguments[3]))
            throw Exception("Fourth argument for function " + getName() + " (timezone) must be String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 3}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto * unit_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
        if (!unit_column)
            throw Exception("First argument for function " + getName() + " must be constant String", ErrorCodes::ILLEGAL_COLUMN);

        String unit = Poco::toLower(unit_column->getValue<String>());

        const IColumn & x = *block.getByPosition(arguments[1]).column;
        const IColumn & y = *block.getByPosition(arguments[2]).column;

        size_t rows = input_rows_count;
        auto res = ColumnInt64::create(rows);

        const DateLUTImpl & timezone_x = extractTimeZoneFromFunctionArguments(block, arguments, 3, 1);
        const DateLUTImpl & timezone_y = extractTimeZoneFromFunctionArguments(block, arguments, 3, 2);

        if (unit == "year" || unit == "yy" || unit == "yyyy")
            dispatchForColumns<ToRelativeYearNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "quarter" || unit == "qq" || unit == "q")
            dispatchForColumns<ToRelativeQuarterNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "month" || unit == "mm" || unit == "m")
            dispatchForColumns<ToRelativeMonthNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "week" || unit == "wk" || unit == "ww")
            dispatchForColumns<ToRelativeWeekNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "day" || unit == "dd" || unit == "d")
            dispatchForColumns<ToRelativeDayNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "hour" || unit == "hh")
            dispatchForColumns<ToRelativeHourNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "minute" || unit == "mi" || unit == "n")
            dispatchForColumns<ToRelativeMinuteNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "second" || unit == "ss" || unit == "s")
            dispatchForColumns<ToRelativeSecondNumImpl>(x, y, timezone_x, timezone_y, res->getData());
        else
            throw Exception("Function " + getName() + " does not support '" + unit + "' unit", ErrorCodes::BAD_ARGUMENTS);

        block.getByPosition(result).column = std::move(res);
    }

private:
    template <typename Transform>
    void dispatchForColumns(
        const IColumn & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * x_vec = checkAndGetColumn<ColumnUInt16>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (auto * x_vec = checkAndGetColumn<ColumnUInt32>(&x))
            dispatchForSecondColumn<Transform>(*x_vec, y, timezone_x, timezone_y, result);
        else if (auto * x_const = checkAndGetColumnConst<ColumnUInt16>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt16>(), y, timezone_x, timezone_y, result);
        else if (auto * x_const = checkAndGetColumnConst<ColumnUInt32>(&x))
            dispatchConstForSecondColumn<Transform>(x_const->getValue<UInt32>(), y, timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for first argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchForSecondColumn(
        const ColumnVector<T1> & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            vector_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            vector_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_const = checkAndGetColumnConst<ColumnUInt16>(&y))
            vector_constant<Transform>(x, y_const->getValue<UInt16>(), timezone_x, timezone_y, result);
        else if (auto * y_const = checkAndGetColumnConst<ColumnUInt32>(&y))
            vector_constant<Transform>(x, y_const->getValue<UInt32>(), timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for second argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1>
    void dispatchConstForSecondColumn(
        T1 x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        if (auto * y_vec = checkAndGetColumn<ColumnUInt16>(&y))
            constant_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else if (auto * y_vec = checkAndGetColumn<ColumnUInt32>(&y))
            constant_vector<Transform>(x, *y_vec, timezone_x, timezone_y, result);
        else
            throw Exception("Illegal column for second argument of function " + getName() + ", must be Date or DateTime", ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename Transform, typename T1, typename T2>
    void vector_vector(
        const ColumnVector<T1> & x, const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void vector_constant(
        const ColumnVector<T1> & x, T2 y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & x_data = x.getData();
        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate<Transform>(x_data[i], y, timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    void constant_vector(
        T1 x, const ColumnVector<T2> & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result)
    {
        const auto & y_data = y.getData();
        for (size_t i = 0, size = y.size(); i < size; ++i)
            result[i] = calculate<Transform>(x, y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename T2>
    Int64 calculate(T1 x, T2 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y)
    {
        return Int64(Transform::execute(y, timezone_y))
             - Int64(Transform::execute(x, timezone_x));
    }
};


/// Get the current time. (It is a constant, it is evaluated once for the entire query.)
class FunctionNow : public IFunction
{
public:
    static constexpr auto name = "now";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionNow>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDateTime>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt32().createColumnConst(
            input_rows_count,
            static_cast<UInt64>(time(nullptr)));
    }
};


class FunctionToday : public IFunction
{
public:
    static constexpr auto name = "today";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToday>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt16().createColumnConst(
            input_rows_count,
            UInt64(DateLUT::instance().toDayNum(time(nullptr))));
    }
};


class FunctionYesterday : public IFunction
{
public:
    static constexpr auto name = "yesterday";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionYesterday>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeDate>();
    }

    bool isDeterministic() const override { return false; }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) override
    {
        block.getByPosition(result).column = DataTypeUInt16().createColumnConst(
            input_rows_count,
            UInt64(DateLUT::instance().toDayNum(time(nullptr)) - 1));
    }
};


/// Just changes time zone information for data type. The calculation is free.
class FunctionToTimeZone : public IFunction
{
public:
    static constexpr auto name = "toTimeZone";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToTimeZone>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!WhichDataType(arguments[0].type).isDateTime())
            throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName() +
                ". Should be DateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        String time_zone_name = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
        return std::make_shared<DataTypeDateTime>(time_zone_name);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};


class FunctionTimeSlot : public IFunction
{
public:
    static constexpr auto name = "timeSlot";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlot>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isDateTime())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeDateTime>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (const ColumnUInt32 * times = typeid_cast<const ColumnUInt32 *>(block.getByPosition(arguments[0]).column.get()))
        {
            auto res = ColumnUInt32::create();
            ColumnUInt32::Container & res_vec = res->getData();
            const ColumnUInt32::Container & vec = times->getData();

            size_t size = vec.size();
            res_vec.resize(size);

            for (size_t i = 0; i < size; ++i)
                res_vec[i] = vec[i] / TIME_SLOT_SIZE * TIME_SLOT_SIZE;

            block.getByPosition(result).column = std::move(res);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


template <typename DurationType>
struct TimeSlotsImpl
{
    static void vector_vector(
        const PaddedPODArray<UInt32> & starts, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void vector_constant(
        const PaddedPODArray<UInt32> & starts, DurationType duration,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = starts.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = starts[i] / TIME_SLOT_SIZE; value <= (starts[i] + duration) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constant_vector(
        UInt32 start, const PaddedPODArray<DurationType> & durations,
        PaddedPODArray<UInt32> & result_values, ColumnArray::Offsets & result_offsets)
    {
        size_t size = durations.size();

        result_offsets.resize(size);
        result_values.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + durations[i]) / TIME_SLOT_SIZE; ++value)
            {
                result_values.push_back(value * TIME_SLOT_SIZE);
                ++current_offset;
            }

            result_offsets[i] = current_offset;
        }
    }

    static void constant_constant(
        UInt32 start, DurationType duration,
        Array & result)
    {
        for (UInt32 value = start / TIME_SLOT_SIZE; value <= (start + duration) / TIME_SLOT_SIZE; ++value)
            result.push_back(static_cast<UInt64>(value * TIME_SLOT_SIZE));
    }
};


class FunctionTimeSlots : public IFunction
{
public:
    static constexpr auto name = "timeSlots";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionTimeSlots>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[0]).isDateTime())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be DateTime.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1]).isUInt32())
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be UInt32.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto starts = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());
        auto const_starts = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[0]).column.get());

        auto durations = checkAndGetColumn<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());
        auto const_durations = checkAndGetColumnConst<ColumnUInt32>(block.getByPosition(arguments[1]).column.get());

        auto res = ColumnArray::create(ColumnUInt32::create());
        ColumnUInt32::Container & res_values = typeid_cast<ColumnUInt32 &>(res->getData()).getData();

        if (starts && durations)
        {
            TimeSlotsImpl<UInt32>::vector_vector(starts->getData(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (starts && const_durations)
        {
            TimeSlotsImpl<UInt32>::vector_constant(starts->getData(), const_durations->getValue<UInt32>(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && durations)
        {
            TimeSlotsImpl<UInt32>::constant_vector(const_starts->getValue<UInt32>(), durations->getData(), res_values, res->getOffsets());
            block.getByPosition(result).column = std::move(res);
        }
        else if (const_starts && const_durations)
        {
            Array const_res;
            TimeSlotsImpl<UInt32>::constant_constant(const_starts->getValue<UInt32>(), const_durations->getValue<UInt32>(), const_res);
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, const_res);
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
                    + ", " + block.getByPosition(arguments[1]).column->getName()
                    + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/** formatDateTime(time, 'pattern')
  * Performs formatting of time, according to provided pattern
  */
class FunctionFormatDateTime : public IFunction
{
private:
    class FormattingOperation
    {
    public:
        using Func = void (*)(char *&, UInt32, const DateLUTImpl &);

        FormattingOperation(Func operation) : operation(operation) {}
        FormattingOperation(StringRef string_to_copy) : string_to_copy(string_to_copy) {}

        Func operation = nullptr;

    private:
        StringRef string_to_copy;

        template <typename T>
        static inline void writeNumber2(char *& p, T v)
        {
            static const char digits[201] =
                "00010203040506070809"
                "10111213141516171819"
                "20212223242526272829"
                "30313233343536373839"
                "40414243444546474849"
                "50515253545556575859"
                "60616263646566676869"
                "70717273747576777879"
                "80818283848586878889"
                "90919293949596979899";

            memcpy(p, &digits[v * 2], 2);
            p += 2;
        }

        template <typename T>
        static inline void writeNumber3(char *& p, T v)
        {
            writeNumber2(p, v / 10);
            *p = '0' + v % 10;
            ++p;
        }

        template <typename T>
        static inline void writeNumber4(char *& p, T v)
        {
            writeNumber2(p, v / 100);
            writeNumber2(p, v % 100);
        }

    public:
        // format DateTime
        void action(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            if (operation)
                operation(target, source, timezone);
            else
                copy(target);
        }

        // format Date (convert to DateTime implicitly)
        void action(char *& target, UInt16 date, const DateLUTImpl & timezone)
        {
            /// TODO This is not efficient.
            const UInt32 datetime = timezone.fromDayNum(DayNum(date));
            action(target, datetime, timezone);
        }

        void copy(char *& target)
        {
            memcpy(target, string_to_copy.data, string_to_copy.size);
            target += string_to_copy.size;
        }

        static void format_C(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto year = ToYearImpl::execute(source, timezone);
            auto century = year / 100;
            writeNumber2(target, century);
        }

        static void format_d(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToDayOfMonthImpl::execute(source, timezone));
        }

        static void format_D(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto month = ToMonthImpl::execute(source, timezone);
            auto day = ToDayOfMonthImpl::execute(source, timezone);
            auto year = ToYearImpl::execute(source, timezone) % 100;
            writeNumber2(target, month);
            *target++ = '/';
            writeNumber2(target, day);
            *target++ = '/';
            writeNumber2(target, year);
        }

        static void format_e(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfMonthImpl::execute(source, timezone);

            if (day < 10)
            {
                *target++ = ' ';
                *target++ = '0' + day;
            }
            else
                writeNumber2(target, day);
        }

        static inline void format_F(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto year = ToYearImpl::execute(source, timezone);
            auto month = ToMonthImpl::execute(source, timezone);
            auto day = ToDayOfMonthImpl::execute(source, timezone);
            writeNumber4(target, year);
            *target++ = '-';
            writeNumber2(target, month);
            *target++ = '-';
            writeNumber2(target, day);
        }

        static void format_H(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto x = ToHourImpl::execute(source, timezone);
            writeNumber2(target, x);
        }

        static void format_I(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto x = ToHourImpl::execute(source, timezone);
            writeNumber2(target, x == 0 ? 12 : (x > 12 ? x - 12 : x));
        }

        /// TODO format_l

        static void format_j(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber3(target, ToDayOfYearImpl::execute(source, timezone));
        }

        static void format_m(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto x = ToMonthImpl::execute(source, timezone);
            writeNumber2(target, x);
        }

        static void format_M(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto x = ToMinuteImpl::execute(source, timezone);
            writeNumber2(target, x);
        }

        static void format_n(char *& target, UInt32 , const DateLUTImpl & )
        {
            *target++ = '\n';
        }

        static void format_p(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            *target++ = ToHourImpl::execute(source, timezone) < 12 ? 'A' : 'P';
            *target++ = 'M';
        }

        // 24-hour HH:MM time, equivalent to %H:%M 14:55
        static void format_R(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto hour = ToHourImpl::execute(source, timezone);
            auto minute = ToMinuteImpl::execute(source, timezone);

            writeNumber2(target, hour);
            *target++ = ':';
            writeNumber2(target, minute);
        }

        static void format_S(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToSecondImpl::execute(source, timezone));
        }

        static void format_t(char *& target, UInt32 , const DateLUTImpl & )
        {
            *target++ = '\t';
        }

        // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S 14:55:02
        static inline void format_T(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToHourImpl::execute(source, timezone));
            *target++ = ':';
            writeNumber2(target, ToMinuteImpl::execute(source, timezone));
            *target++ = ':';
            writeNumber2(target, ToSecondImpl::execute(source, timezone));
        }

        // ISO 8601 weekday as number with Monday as 1 (1-7)
        static void format_u(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            *target++ = '0' + ToDayOfWeekImpl::execute(source, timezone);
        }

        // Week number with the first Sunday as the first day of week one (00-53)
        static void format_U(char *& , UInt32 , const DateLUTImpl & )
        {
            throw Exception("Handler %U is not implemented", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // ISO 8601 week number (01-53)
        static void format_V(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToISOWeekImpl::execute(source, timezone));
        }

        // Week number with the first Monday as the first day of week one (00-53)
        static void format_W(char *& , UInt32 , const DateLUTImpl & )
        {
            throw Exception("Handler %W is not implemented", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        // Weekday as a decimal number with Sunday as 0 (0-6)
        static void format_w(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            auto day = ToDayOfWeekImpl::execute(source, timezone);
            *target++ = '0' + (day == 7 ? 0 : day);
        }

        // Year, last two digits (00-99)
        static void format_y(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber2(target, ToYearImpl::execute(source, timezone) % 100);
        }

        // Year, 4 digits
        static void format_Y(char *& target, UInt32 source, const DateLUTImpl & timezone)
        {
            writeNumber4(target, ToYearImpl::execute(source, timezone));
        }

        static void format_Percent(char *& target, UInt32 , const DateLUTImpl &)
        {
            *target++ = '%';
        }
    };

public:
    static constexpr auto name = "formatDateTime";

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFormatDateTime>(); }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                            + toString(arguments.size()) + ", should be 2 or 3",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!WhichDataType(arguments[0].type).isDateOrDateTime())
            throw Exception("Illegal type " + arguments[0].type->getName() + " of 1 argument of function " + getName() +
                            ". Should be a date or a date with time", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!WhichDataType(arguments[1].type).isString())
            throw Exception("Illegal type " + arguments[1].type->getName() + " of 2 argument of function " + getName() + ". Must be String.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3)
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception("Illegal type " + arguments[2].type->getName() + " of 3 argument of function " + getName() + ". Must be String.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (!executeType<UInt32>(block, arguments, result) && !executeType<UInt16>(block, arguments, result))
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + "  of function " + getName() + ", must be Date or DateTime",
                            ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename T>
    bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (auto * times = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            const ColumnConst * pattern_column = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get());

            if (!pattern_column)
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                                + " of second ('format') argument of function " + getName()
                                + ". Must be constant string.",
                                ErrorCodes::ILLEGAL_COLUMN);

            String pattern = pattern_column->getValue<String>();

            std::vector<FormattingOperation> instructions = {};
            size_t result_length = parsePattern(pattern, instructions);

            const DateLUTImpl * time_zone_tmp = nullptr;
            if (arguments.size() == 3)
                time_zone_tmp = &extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);
            else
                time_zone_tmp = &DateLUT::instance();

            const DateLUTImpl & time_zone = *time_zone_tmp;

            const typename ColumnVector<T>::Container & vec = times->getData();

            auto col_res = ColumnString::create();
            auto & dst_data = col_res->getChars();
            auto & dst_offsets = col_res->getOffsets();
            dst_data.resize(vec.size() * (result_length + 1));
            dst_offsets.resize(vec.size());

            auto begin = reinterpret_cast<char *>(dst_data.data());
            auto pos = begin;

            for (size_t i = 0; i < vec.size(); ++i)
            {
                for(auto & instruction : instructions)
                    instruction.action(pos, vec[i], time_zone);

                *pos++ = '\0';
                dst_offsets[i] = pos - begin;
            }

            dst_data.resize(pos - begin);
            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    size_t parsePattern(String & pattern, std::vector<FormattingOperation> & instructions)
    {
        char * begin_of_pattern = pattern.data();
        size_t result_size = 0;
        size_t last_pos = 0;

        for (size_t s = 0; s < pattern.length(); ++s)
        {
            if (pattern[s] == '%')
            {
                if (last_pos > 0)
                {
                    auto length = 1 + s - last_pos;
                    instructions.emplace_back(StringRef(begin_of_pattern + last_pos - 1, length));
                    last_pos = 0;
                    result_size += length;
                }

                FormattingOperation::Func operation;

                if (++s == pattern.length())
                    throw Exception("Sign '%' is last in pattern, if you need it, use '%%'", ErrorCodes::BAD_ARGUMENTS);

                switch (pattern[s])
                {
                    // Year, divided by 100, zero-padded
                    case 'C':
                        operation = &FormattingOperation::format_C;
                        result_size += 2;
                        break;

                    // Day of the month, zero-padded (01-31)
                    case 'd':
                        operation = &FormattingOperation::format_d;
                        result_size += 2;
                        break;

                    // Short MM/DD/YY date, equivalent to %m/%d/%y
                    case 'D':
                        operation = &FormattingOperation::format_D;
                        result_size += 8;
                        break;

                    // Day of the month, space-padded ( 1-31)  23
                    case 'e':
                        operation = &FormattingOperation::format_e;
                        result_size += 2;
                    break;

                    // Short YYYY-MM-DD date, equivalent to %Y-%m-%d   2001-08-23
                    case 'F':
                        operation = &FormattingOperation::format_F;
                        result_size += 10;
                    break;

                    // Hour in 24h format (00-23)
                    case 'H':
                        operation = &FormattingOperation::format_H;
                        result_size += 2;
                        break;

                    // Hour in 12h format (01-12)
                    case 'I':
                        operation = &FormattingOperation::format_I;
                        result_size += 2;
                        break;

                    // Day of the year (001-366)   235
                    case 'j':
                        operation = &FormattingOperation::format_j;
                        result_size += 3;
                        break;

                    // Month as a decimal number (01-12)
                    case 'm':
                        operation = &FormattingOperation::format_m;
                        result_size += 2;
                        break;

                    // Minute (00-59)
                    case 'M':
                        operation = &FormattingOperation::format_M;
                        result_size += 2;
                        break;

                    // New line character "\n"
                    case 'n':
                        operation = &FormattingOperation::format_n;
                        result_size += 1;
                        break;

                    // AM or PM
                    case 'p':
                        operation = &FormattingOperation::format_p;
                        result_size += 2;
                        break;

                    // 24-hour HH:MM time, equivalent to %H:%M 14:55
                    case 'R':
                        operation = &FormattingOperation::format_R;
                        result_size += 5;
                        break;

                    // Seconds
                    case 'S':
                        operation = &FormattingOperation::format_S;
                        result_size += 2;
                        break;

                    // Horizontal-tab character ('\t')
                    case 't':
                        operation = &FormattingOperation::format_t;
                        result_size += 1;
                        break;

                    // ISO 8601 time format (HH:MM:SS), equivalent to %H:%M:%S 14:55:02
                    case 'T':
                        operation = &FormattingOperation::format_T;
                        result_size += 8;
                        break;

                    // ISO 8601 weekday as number with Monday as 1 (1-7)
                    case 'u':
                        operation = &FormattingOperation::format_u;
                        result_size += 1;
                        break;

                    // Week number with the first Sunday as the first day of week one (00-53)
                    case 'U':
                        operation = &FormattingOperation::format_U;
                        result_size += 2;
                        break;

                    // ISO 8601 week number (01-53)
                    case 'V':
                        operation = &FormattingOperation::format_V;
                        result_size += 2;
                        break;

                    // Weekday as a decimal number with Sunday as 0 (0-6)  4
                    case 'w':
                        operation = &FormattingOperation::format_w;
                        result_size += 1;
                        break;

                    // Week number with the first Monday as the first day of week one (00-53)
                    case 'W':
                        operation = &FormattingOperation::format_W;
                        result_size += 2;
                        break;

                        // Four digits year
                    case 'y':
                        operation = &FormattingOperation::format_y;
                        result_size += 2;

                        break;

                    // Four digits year
                    case 'Y':
                        operation = &FormattingOperation::format_Y;
                        result_size += 4;

                        break;

                    case '%':
                        operation = &FormattingOperation::format_Percent;
                        result_size += 1;
                        break;

                    default:
                        throw Exception(
                            "Wrong pattern '" + pattern + "', unexpected symbol '" + pattern[s] + "' for function " + getName(), ErrorCodes::ILLEGAL_COLUMN);
                }

                instructions.emplace_back(operation);
            }
            else
            {
                if (last_pos == 0)
                    last_pos = s + 1;
            }
        }

        if (last_pos > 0)
        {
            auto length = 1 + pattern.length() - last_pos;
            instructions.emplace_back(StringRef(begin_of_pattern + last_pos - 1, length));
            result_size += length;
        }

        return result_size;
    }
};


using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;
using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;
using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;
using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;
using FunctionToDayOfWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfWeekImpl>;
using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;
using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToHourImpl>;
using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMinuteImpl>;
using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToSecondImpl>;
using FunctionToStartOfDay = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfDayImpl>;
using FunctionToMonday = FunctionDateOrDateTimeToSomething<DataTypeDate, ToMondayImpl>;
using FunctionToISOWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToISOWeekImpl>;
using FunctionToISOYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToISOYearImpl>;
using FunctionToStartOfMonth = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfMonthImpl>;
using FunctionToStartOfQuarter = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfQuarterImpl>;
using FunctionToStartOfYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfYearImpl>;
using FunctionToStartOfMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfMinuteImpl>;
using FunctionToStartOfFiveMinute = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFiveMinuteImpl>;
using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfFifteenMinutesImpl>;
using FunctionToStartOfHour = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToStartOfHourImpl>;
using FunctionToStartOfISOYear = FunctionDateOrDateTimeToSomething<DataTypeDate, ToStartOfISOYearImpl>;
using FunctionToTime = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeImpl>;

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl>;
using FunctionToRelativeQuarterNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeQuarterNumImpl>;
using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMonthNumImpl>;
using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl>;
using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl>;
using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl>;
using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl>;
using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl>;

using FunctionToYYYYMM = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMImpl>;
using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToYYYYMMDDImpl>;
using FunctionToYYYYMMDDhhmmss = FunctionDateOrDateTimeToSomething<DataTypeUInt64, ToYYYYMMDDhhmmssImpl>;

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;
using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;
using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;
using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;
using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;
using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;
using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

using FunctionSubtractSeconds = FunctionDateOrDateTimeAddInterval<SubtractSecondsImpl>;
using FunctionSubtractMinutes = FunctionDateOrDateTimeAddInterval<SubtractMinutesImpl>;
using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;
using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;
using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;
using FunctionSubtractMonths = FunctionDateOrDateTimeAddInterval<SubtractMonthsImpl>;
using FunctionSubtractYears = FunctionDateOrDateTimeAddInterval<SubtractYearsImpl>;

}
