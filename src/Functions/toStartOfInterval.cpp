#include <Common/DateLUTImpl.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/TransformDateTime64.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


namespace
{
    constexpr auto function_name = "toStartOfInterval";

    template <IntervalKind::Kind unit>
    struct Transform;

    template <>
    struct Transform<IntervalKind::Year>
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

        static UInt16 execute(Int64 t, Int64 years, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfYearInterval(time_zone.toDayNum(t / scale_multiplier), years);
        }
    };

    template <>
    struct Transform<IntervalKind::Quarter>
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

        static UInt16 execute(Int64 t, Int64 quarters, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t / scale_multiplier), quarters);
        }
    };

    template <>
    struct Transform<IntervalKind::Month>
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

        static UInt16 execute(Int64 t, Int64 months, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfMonthInterval(time_zone.toDayNum(t / scale_multiplier), months);
        }
    };

    template <>
    struct Transform<IntervalKind::Week>
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

        static UInt16 execute(Int64 t, Int64 weeks, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t / scale_multiplier), weeks);
        }
    };

    template <>
    struct Transform<IntervalKind::Day>
    {
        static UInt32 execute(UInt16 d, Int64 days, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfDayInterval(ExtendedDayNum(d), days);
        }

        static UInt32 execute(Int32 d, Int64 days, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfDayInterval(ExtendedDayNum(d), days);
        }

        static UInt32 execute(UInt32 t, Int64 days, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfDayInterval(time_zone.toDayNum(t), days);
        }

        static Int64 execute(Int64 t, Int64 days, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfDayInterval(time_zone.toDayNum(t / scale_multiplier), days);
        }
    };

    template <>
    struct Transform<IntervalKind::Hour>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 hours, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfHourInterval(t, hours);
        }

        static UInt32 execute(Int64 t, Int64 hours, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfHourInterval(t / scale_multiplier, hours);
        }
    };

    template <>
    struct Transform<IntervalKind::Minute>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 minutes, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfMinuteInterval(t, minutes);
        }

        static UInt32 execute(Int64 t, Int64 minutes, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfMinuteInterval(t / scale_multiplier, minutes);
        }
    };

    template <>
    struct Transform<IntervalKind::Second>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, Int64 seconds, const DateLUTImpl & time_zone, Int64)
        {
            return time_zone.toStartOfSecondInterval(t, seconds);
        }

        static UInt32 execute(Int64 t, Int64 seconds, const DateLUTImpl & time_zone, Int64 scale_multiplier)
        {
            return time_zone.toStartOfSecondInterval(t / scale_multiplier, seconds);
        }
    };

    template <>
    struct Transform<IntervalKind::Millisecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { return dateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 milliseconds, const DateLUTImpl &, Int64 scale_multiplier)
        {
            if (scale_multiplier < 1000)
            {
                Int64 t_milliseconds = t * (static_cast<Int64>(1000) / scale_multiplier);
                if (likely(t >= 0))
                    return t_milliseconds / milliseconds * milliseconds;
                else
                    return ((t_milliseconds + 1) / milliseconds - 1) * milliseconds;
            }
            else if (scale_multiplier > 1000)
            {
                Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000);
                if (likely(t >= 0))
                    return t / milliseconds / scale_diff * milliseconds;
                else
                    return ((t + 1) / milliseconds / scale_diff - 1) * milliseconds;
            }
            else
                if (likely(t >= 0))
                    return t / milliseconds * milliseconds;
                else
                    return ((t + 1) / milliseconds - 1) * milliseconds;
        }
    };

    template <>
    struct Transform<IntervalKind::Microsecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { return dateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 microseconds, const DateLUTImpl &, Int64 scale_multiplier)
        {
            if (scale_multiplier < 1000000)
            {
                Int64 t_microseconds = t * (static_cast<Int64>(1000000) / scale_multiplier);
                if (likely(t >= 0))
                    return t_microseconds / microseconds * microseconds;
                else
                    return ((t_microseconds + 1) / microseconds - 1) * microseconds;
            }
            else if (scale_multiplier > 1000000)
            {
                Int64 scale_diff = scale_multiplier / static_cast<Int64>(1000000);
                if (likely(t >= 0))
                    return t / microseconds / scale_diff * microseconds;
                else
                    return ((t + 1) / microseconds / scale_diff - 1) * microseconds;
            }
            else
                if (likely(t >= 0))
                    return t / microseconds * microseconds;
                else
                    return ((t + 1) / microseconds - 1) * microseconds;
        }
    };

    template <>
    struct Transform<IntervalKind::Nanosecond>
    {
        static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { return dateTimeIsNotSupported(function_name); }

        static Int64 execute(Int64 t, Int64 nanoseconds, const DateLUTImpl &, Int64 scale_multiplier)
        {
            if (scale_multiplier < 1000000000)
            {
                Int64 t_nanoseconds = t * (static_cast<Int64>(1000000000) / scale_multiplier);
                if (likely(t >= 0))
                    return t_nanoseconds / nanoseconds * nanoseconds;
                else
                    return ((t_nanoseconds + 1) / nanoseconds - 1) * nanoseconds;
            }
            else
                if (likely(t >= 0))
                    return t / nanoseconds * nanoseconds;
                else
                    return ((t + 1) / nanoseconds - 1) * nanoseconds;
        }
    };

class FunctionToStartOfInterval : public IFunction
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToStartOfInterval>(); }

    static constexpr auto name = function_name;
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }


    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool first_argument_is_date = false;
        auto check_first_argument = [&]
        {
            if (!isDate(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            first_argument_is_date = isDate(arguments[0].type);
        };

        const DataTypeInterval * interval_type = nullptr;
        bool result_type_is_date = false;
        bool result_type_is_datetime = false;
        auto check_interval_argument = [&]
        {
            interval_type = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());
            if (!interval_type)
                throw Exception(
                    "Illegal type " + arguments[1].type->getName() + " of argument of function " + getName()
                        + ". Should be an interval of time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            result_type_is_date = (interval_type->getKind() == IntervalKind::Year)
                || (interval_type->getKind() == IntervalKind::Quarter) || (interval_type->getKind() == IntervalKind::Month)
                || (interval_type->getKind() == IntervalKind::Week);
            result_type_is_datetime = (interval_type->getKind() == IntervalKind::Day) || (interval_type->getKind() == IntervalKind::Hour)
                || (interval_type->getKind() == IntervalKind::Minute) || (interval_type->getKind() == IntervalKind::Second);
        };

        auto check_timezone_argument = [&]
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Illegal type " + arguments[2].type->getName() + " of argument of function " + getName()
                        + ". This argument is optional and must be a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (first_argument_is_date && result_type_is_date)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} with interval type {} is allowed only when the 1st argument "
                    "has the type DateTime or DateTime64",
                        getName(), interval_type->getKind().toString());
        };

        if (arguments.size() == 2)
        {
            check_first_argument();
            check_interval_argument();
        }
        else if (arguments.size() == 3)
        {
            check_first_argument();
            check_interval_argument();
            check_timezone_argument();
        }
        else
        {
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 2 or 3",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        if (result_type_is_date)
            return std::make_shared<DataTypeDate>();
        else if (result_type_is_datetime)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        else
        {
            auto scale = 0;

            if (interval_type->getKind() == IntervalKind::Nanosecond)
                scale = 9;
            else if (interval_type->getKind() == IntervalKind::Microsecond)
                scale = 6;
            else if (interval_type->getKind() == IntervalKind::Millisecond)
                scale = 3;

            return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
        }

    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /* input_rows_count */) const override
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        auto result_column = dispatchForColumns(time_column, interval_column, result_type, time_zone);
        return result_column;
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

private:
    ColumnPtr dispatchForColumns(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & from_datatype = *time_column.type.get();
        const auto which_type = WhichDataType(from_datatype);

        if (which_type.isDateTime64())
        {
            const auto * time_column_vec = checkAndGetColumn<DataTypeDateTime64::ColumnType>(time_column.column.get());
            auto scale = assert_cast<const DataTypeDateTime64 &>(from_datatype).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64&>(from_datatype), *time_column_vec, interval_column, result_type, time_zone, scale);
        }
        if (which_type.isDateTime())
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime&>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (which_type.isDate())
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt16>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate&>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (which_type.isDate32())
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnInt32>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate32&>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        throw Exception(
            "Illegal column for first argument of function " + getName() + ". Must contain dates or dates with time",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <typename ColumnType, typename FromDataType>
    ColumnPtr dispatchForIntervalColumn(
        const FromDataType & from, const ColumnType & time_column, const ColumnWithTypeAndName & interval_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale = 1) const
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(
                "Illegal column for second argument of function " + getName() + ", must be an interval of time.",
                ErrorCodes::ILLEGAL_COLUMN);
        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(
                "Illegal column for second argument of function " + getName() + ", must be a const interval of time.", ErrorCodes::ILLEGAL_COLUMN);
        Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception("Value for second argument of function " + getName() + " must be positive.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        switch (interval_type->getKind())
        {
            case IntervalKind::Nanosecond:
                return execute<FromDataType, DataTypeDateTime64, IntervalKind::Nanosecond>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Microsecond:
                return execute<FromDataType, DataTypeDateTime64, IntervalKind::Microsecond>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Millisecond:
                return execute<FromDataType, DataTypeDateTime64, IntervalKind::Millisecond>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Second:
                return execute<FromDataType, DataTypeDateTime, IntervalKind::Second>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Minute:
                return execute<FromDataType, DataTypeDateTime, IntervalKind::Minute>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Hour:
                return execute<FromDataType, DataTypeDateTime, IntervalKind::Hour>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Day:
                return execute<FromDataType, DataTypeDateTime, IntervalKind::Day>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Week:
                return execute<FromDataType, DataTypeDate, IntervalKind::Week>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Month:
                return execute<FromDataType, DataTypeDate, IntervalKind::Month>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Quarter:
                return execute<FromDataType, DataTypeDate, IntervalKind::Quarter>(from, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Year:
                return execute<FromDataType, DataTypeDate, IntervalKind::Year>(from, time_column, num_units, result_type, time_zone, scale);
        }

        __builtin_unreachable();
    }

    template <typename FromDataType, typename ToDataType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const FromDataType &, const ColumnType & time_column_type, Int64 num_units, const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale) const
    {
        using ToColumnType = typename ToDataType::ColumnType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto *col_to = assert_cast<ToColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        Int64 scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

        for (size_t i = 0; i != size; ++i)
            result_data[i] = Transform<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier);

        return result_col;
    }
};

}

void registerFunctionToStartOfInterval(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
