#include <base/arithmeticOverflow.h>
#include <Common/Exception.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
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

    static Int64 execute(Int64 t, Int64 days, const DateLUTImpl & time_zone, Int64 scale_multiplier)
    {
        return time_zone.toStartOfDayInterval(time_zone.toDayNum(t / scale_multiplier), days);
    }
};

template <>
struct Transform<IntervalKind::Hour>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32 t, Int64 hours, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfHourInterval(t, hours);
    }

    static Int64 execute(Int64 t, Int64 hours, const DateLUTImpl & time_zone, Int64 scale_multiplier)
    {
        return time_zone.toStartOfHourInterval(t / scale_multiplier, hours);
    }
};

template <>
struct Transform<IntervalKind::Minute>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32 t, Int64 minutes, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfMinuteInterval(t, minutes);
    }

    static Int64 execute(Int64 t, Int64 minutes, const DateLUTImpl & time_zone, Int64 scale_multiplier)
    {
        return time_zone.toStartOfMinuteInterval(t / scale_multiplier, minutes);
    }
};

template <>
struct Transform<IntervalKind::Second>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32 t, Int64 seconds, const DateLUTImpl & time_zone, Int64)
    {
        return time_zone.toStartOfSecondInterval(t, seconds);
    }

    static Int64 execute(Int64 t, Int64 seconds, const DateLUTImpl & time_zone, Int64 scale_multiplier)
    {
        return time_zone.toStartOfSecondInterval(t / scale_multiplier, seconds);
    }
};

template <>
struct Transform<IntervalKind::Millisecond>
{
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { throwDateTimeIsNotSupported(function_name); }

    static Int64 execute(Int64 t, Int64 milliseconds, const DateLUTImpl &, Int64 scale_multiplier)
    {
        if (scale_multiplier < 1000)
        {
            Int64 t_milliseconds = 0;
            if (common::mulOverflow(t, static_cast<Int64>(1000) / scale_multiplier, t_milliseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
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
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { throwDateTimeIsNotSupported(function_name); }

    static Int64 execute(Int64 t, Int64 microseconds, const DateLUTImpl &, Int64 scale_multiplier)
    {
        if (scale_multiplier < 1000000)
        {
            Int64 t_microseconds = 0;
            if (common::mulOverflow(t, static_cast<Int64>(1000000) / scale_multiplier, t_microseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
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
    static UInt32 execute(UInt16, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(Int32, Int64, const DateLUTImpl &, Int64) { throwDateIsNotSupported(function_name); }

    static UInt32 execute(UInt32, Int64, const DateLUTImpl &, Int64) { throwDateTimeIsNotSupported(function_name); }

    static Int64 execute(Int64 t, Int64 nanoseconds, const DateLUTImpl &, Int64 scale_multiplier)
    {
        if (scale_multiplier < 1000000000)
        {
            Int64 t_nanoseconds = 0;
            if (common::mulOverflow(t, (static_cast<Int64>(1000000000) / scale_multiplier), t_nanoseconds))
                throw DB::Exception(ErrorCodes::DECIMAL_OVERFLOW, "Numeric overflow");
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

    static constexpr auto name = "toStartOfInterval";
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }

    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override { return { .is_monotonic = true, .is_always_monotonic = true }; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool value_is_date = false;
        auto check_first_argument = [&]
        {
            const DataTypePtr & type_arg1 = arguments[0].type;
            if (!isDate(type_arg1) && !isDateTime(type_arg1) && !isDateTime64(type_arg1))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 1st argument of function {}. "
                    "Should be a date or a date with time", type_arg1->getName(), getName());
            value_is_date = isDate(type_arg1);
        };

        const DataTypeInterval * interval_type = nullptr;
        enum class ResultType
        {
            Date,
            DateTime,
            DateTime64
        };
        ResultType result_type;
        auto check_second_argument = [&]
        {
            const DataTypePtr & type_arg2 = arguments[1].type;
            interval_type = checkAndGetDataType<DataTypeInterval>(type_arg2.get());
            if (!interval_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 2nd argument of function {}. "
                    "Should be an interval of time", type_arg2->getName(), getName());
            switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
            {
                case IntervalKind::Nanosecond:
                case IntervalKind::Microsecond:
                case IntervalKind::Millisecond:
                    result_type = ResultType::DateTime64;
                    break;
                case IntervalKind::Second:
                case IntervalKind::Minute:
                case IntervalKind::Hour:
                case IntervalKind::Day:
                    result_type = ResultType::DateTime;
                    break;
                case IntervalKind::Week:
                case IntervalKind::Month:
                case IntervalKind::Quarter:
                case IntervalKind::Year:
                    result_type = ResultType::Date;
                    break;
            }
        };

        enum class ThirdArgument
        {
            IsTimezone,
            IsOrigin
        };
        ThirdArgument third_argument; /// valid only if 3rd argument is given
        auto check_third_argument = [&]
        {
            const DataTypePtr & type_arg3 = arguments[2].type;
            if (isString(type_arg3))
            {
                third_argument = ThirdArgument::IsTimezone;
                if (value_is_date && result_type == ResultType::Date)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The timezone argument of function {} with interval type {} is allowed only when the 1st argument has the type DateTime or DateTime64",
                        getName(), interval_type->getKind().toString());
            }
            else if (isDateOrDate32OrDateTimeOrDateTime64(type_arg3))
                third_argument = ThirdArgument::IsOrigin;
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 3rd argument of function {}. "
                    "This argument is optional and must be a constant String with timezone name or a Date/Date32/DateTime/DateTime64 with a constant origin",
                    type_arg3->getName(), getName());

        };

        auto check_fourth_argument = [&]
        {
            if (third_argument != ThirdArgument::IsOrigin) /// sanity check
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 3rd argument of function {}. "
                    "The third argument must a Date/Date32/DateTime/DateTime64 with a constant origin",
                    arguments[2].type->getName(), getName());

            const DataTypePtr & type_arg4 = arguments[3].type;
            if (!isString(type_arg4))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 4th argument of function {}. "
                    "This argument is optional and must be a constant String with timezone name",
                    type_arg4->getName(), getName());
            if (value_is_date && result_type == ResultType::Date)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} with interval type {} is allowed only when the 1st argument has the type DateTime or DateTime64",
                    getName(), interval_type->getKind().toString());
        };

        if (arguments.size() == 2)
        {
            check_first_argument();
            check_second_argument();
        }
        else if (arguments.size() == 3)
        {
            check_first_argument();
            check_second_argument();
            check_third_argument();
        }
        else if (arguments.size() == 4)
        {
            check_first_argument();
            check_second_argument();
            check_third_argument();
            check_fourth_argument();
        }
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2, 3 or 4",
                getName(), arguments.size());
        }

        switch (result_type)
        {
            case ResultType::Date:
                return std::make_shared<DataTypeDate>();
            case ResultType::DateTime:
            {
                const size_t time_zone_arg_num = (arguments.size() == 2 || (arguments.size() == 3 && third_argument == ThirdArgument::IsTimezone)) ? 2 : 3;
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, time_zone_arg_num, 0, false));
            }
            case ResultType::DateTime64:
            {
                UInt32 scale = 0;
                if (interval_type->getKind() == IntervalKind::Nanosecond)
                    scale = 9;
                else if (interval_type->getKind() == IntervalKind::Microsecond)
                    scale = 6;
                else if (interval_type->getKind() == IntervalKind::Millisecond)
                    scale = 3;

                const size_t time_zone_arg_num = (arguments.size() == 2 || (arguments.size() == 3 && third_argument == ThirdArgument::IsTimezone)) ? 2 : 3;
                return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, time_zone_arg_num, 0, false));
            }
        }

        std::unreachable();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /* input_rows_count */) const override
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];

        ColumnWithTypeAndName origin_column;
        const bool has_origin_arg = (arguments.size() == 3 && isDateOrDate32OrDateTimeOrDateTime64(arguments[2].type)) || arguments.size() == 4;
        if (has_origin_arg)
            origin_column = arguments[2];

        const size_t time_zone_arg_num = (arguments.size() == 3 && isString(arguments[2].type)) ? 2 : 3;
        const auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, time_zone_arg_num, 0);

        auto result_column = dispatchForTimeColumn(time_column, interval_column, origin_column, result_type, time_zone);
        return result_column;
    }

private:
    ColumnPtr dispatchForTimeColumn(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & time_column_type = *time_column.type.get();
        const auto & time_column_col = *time_column.column.get();

        if (isDateTime64(time_column_type))
        {
            if (origin_column.column != nullptr)
                if (!isDateTime64(origin_column.type.get()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same type", getName());

            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column_col);
            auto scale = assert_cast<const DataTypeDateTime64 &>(time_column_type).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone, scale);
        }
        else if (isDateTime(time_column_type))
        {
            if (origin_column.column != nullptr)
                if (!isDateTime(origin_column.type.get()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same type", getName());

            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDate(time_column_type))
        {
            if (origin_column.column != nullptr)
                if (!isDate(origin_column.type.get()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same type", getName());

            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDate32(time_column_type))
        {
            if (origin_column.column != nullptr)
                if (!isDate32(origin_column.type.get()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same type", getName());

            const auto * time_column_vec = checkAndGetColumn<ColumnDate32>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate32 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for first argument of function {}. Must contain dates or dates with time", getName());
    }

    template <typename TimeColumnType, typename TimeDataType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column, const ColumnWithTypeAndName & origin_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale = 1) const
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for second argument of function {}, must be an interval of time.", getName());

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for second argument of function {}, must be a const interval of time.", getName());

        Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value for second argument of function {} must be positive.", getName());

        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Nanosecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Nanosecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Microsecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Microsecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Millisecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Millisecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Second:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Second>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Minute:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Minute>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Hour:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Hour>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Day:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Day>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Week:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Week>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Month:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Month>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Quarter:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Quarter>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Year:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Year>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
        }

        std::unreachable();
    }

    template <typename TimeDataType, typename ToDataType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const TimeDataType &, const ColumnType & time_column_type, Int64 num_units, const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale) const
    {
        using ToColumnType = typename ToDataType::ColumnType;
        using ToFieldType = typename ToDataType::FieldType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto * col_to = assert_cast<ToColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        Int64 scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

        if (origin_column.column == nullptr)
            for (size_t i = 0; i != size; ++i)
                result_data[i] = static_cast<ToFieldType>(Transform<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier));
        else
        {
            UInt64 od = origin_column.column->get64(0);

            for (size_t i = 0; i != size; ++i)
            {
                auto td = time_data[i];
                if (od > size_t(td))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The origin must be before the end date/datetime");
                td -= od;
                result_data[i] = static_cast<ToFieldType>(Transform<unit>::execute(td, num_units, time_zone, scale_multiplier));

                result_data[i] += scale_multiplier == 10 ? od : od / scale_multiplier;
            }
        }

        return result_col;
    }
};

}

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
