#include <type_traits>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <Common/IntervalKind.h>
#include "DataTypes/IDataType.h"
#include "base/types.h"
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>


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


class FunctionToStartOfInterval : public IFunction
{
private:
    enum class Overload
    {
        Default,    /// toStartOfInterval(time, interval) or toStartOfInterval(time, interval, timezone)
        Origin      /// toStartOfInterval(time, interval, origin) or toStartOfInterval(time, interval, origin, timezone)
    };
    mutable Overload overload;

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
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 1st argument of function {}, expected a Date, DateTime or DateTime64",
                    type_arg1->getName(), getName());
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
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd argument of function {}, expected a time interval",
                    type_arg2->getName(), getName());

            overload = Overload::Default;

            /// Determine result type for default overload (no origin)
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
                case IntervalKind::Day: /// weird why Day leads to DateTime but too afraid to change it
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

        auto check_third_argument = [&]
        {
            const DataTypePtr & type_arg3 = arguments[2].type;
            if (isString(type_arg3))
            {
                if (value_is_date && result_type == ResultType::Date)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "A timezone argument of function {} with interval type {} is allowed only when the 1st argument has the type DateTime or DateTime64",
                        getName(), interval_type->getKind().toString());
            }
            else if (isDate(type_arg3) || isDateTime(type_arg3) || isDateTime64(type_arg3))
            {
                overload = Overload::Origin;
                const DataTypePtr & type_arg1 = arguments[0].type;
                if (isDateTime64(type_arg1) && isDateTime64(type_arg3))
                    result_type = ResultType::DateTime64;
                else if (isDateTime(type_arg1) && isDateTime(type_arg3))
                    result_type = ResultType::DateTime;
                else if (isDate(type_arg1) && isDate(type_arg3))
                    result_type = ResultType::Date;
                else
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same type", getName());
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of 3rd argument of function {}. "
                    "This argument is optional and must be a constant String with timezone name or a Date/Date32/DateTime/DateTime64 with a constant origin",
                    type_arg3->getName(), getName());
        };

        auto check_fourth_argument = [&]
        {
            if (overload != Overload::Origin) /// sanity check
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
                    "A timezone argument of function {} with interval type {} is allowed only when the 1st argument has the type DateTime or DateTime64",
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
                const size_t time_zone_arg_num = (overload == Overload::Default) ? 2 : 3;
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, time_zone_arg_num, 0, false));
            }
            case ResultType::DateTime64:
            {
                UInt32 scale = 0;
                if (isDateTime64(arguments[0].type))
                    scale = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type.get()).getScale();
                if (interval_type->getKind() == IntervalKind::Nanosecond)
                    scale = (9 > scale) ? 9 : scale;
                else if (interval_type->getKind() == IntervalKind::Microsecond)
                    scale = (6 > scale) ? 6 : scale;
                else if (interval_type->getKind() == IntervalKind::Millisecond)
                    scale = (3 > scale) ? 3 : scale;

                const size_t time_zone_arg_num = (overload == Overload::Default) ? 2 : 3;
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
        if (overload == Overload::Origin)
            origin_column = arguments[2];

        const size_t time_zone_arg_num = (overload == Overload::Origin) ? 3 : 2;
        const auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, time_zone_arg_num, 0);

        ColumnPtr result_column;
        if (isDate(result_type))
            result_column = dispatchForTimeColumn<DataTypeDate>(time_column, interval_column, origin_column, result_type, time_zone);
        else if (isDateTime(result_type))
            result_column = dispatchForTimeColumn<DataTypeDateTime>(time_column, interval_column, origin_column, result_type, time_zone);
        else if (isDateTime64(result_type))
            result_column = dispatchForTimeColumn<DataTypeDateTime64>(time_column, interval_column, origin_column, result_type, time_zone);
        return result_column;
    }

private:
    template <typename ReturnType>
    ColumnPtr dispatchForTimeColumn(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & time_column_type = *time_column.type.get();
        const auto & time_column_col = *time_column.column.get();

        if (isDate(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType>(assert_cast<const DataTypeDate &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDateTime(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType>(assert_cast<const DataTypeDateTime &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDateTime64(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column_col);
            auto scale = assert_cast<const DataTypeDateTime64 &>(time_column_type).getScale();
            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType>(assert_cast<const DataTypeDateTime64 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone, scale);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for 1st argument of function {}, expected a Date, DateTime or DateTime64", getName());
    }

    template <typename ReturnType, typename TimeColumnType, typename TimeDataType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column, const ColumnWithTypeAndName & origin_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, UInt16 scale = 1) const
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for 2nd argument of function {}, must be a time interval", getName());

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for 2nd argument of function {}, must be a const time interval", getName());

        const Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value for 2nd argument of function {} must be positive", getName());

        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Nanosecond:
                return execute<TimeDataType, ReturnType, IntervalKind::Nanosecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Microsecond:
                return execute<TimeDataType, ReturnType, IntervalKind::Microsecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Millisecond:
                return execute<TimeDataType, ReturnType, IntervalKind::Millisecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Second:
                return execute<TimeDataType, ReturnType, IntervalKind::Second>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Minute:
                return execute<TimeDataType, ReturnType, IntervalKind::Minute>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Hour:
                return execute<TimeDataType, ReturnType, IntervalKind::Hour>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Day:
                return execute<TimeDataType, ReturnType, IntervalKind::Day>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Week:
                return execute<TimeDataType, ReturnType, IntervalKind::Week>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Month:
                return execute<TimeDataType, ReturnType, IntervalKind::Month>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Quarter:
                return execute<TimeDataType, ReturnType, IntervalKind::Quarter>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Year:
                return execute<TimeDataType, ReturnType, IntervalKind::Year>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
        }

        std::unreachable();
    }

    template <IntervalKind::Kind unit>
    static Int64 scaleFromInterval()
    {
        switch (unit)
        {
            case IntervalKind::Millisecond:
                return 1'000;
            case IntervalKind::Microsecond:
                return 1'000'000;
            case IntervalKind::Nanosecond:
                return 1'000'000'000;
            default:
                return 1;
        }
    }

    template <typename TimeDataType, typename ResultDataType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const TimeDataType &, const ColumnType & time_column_type, Int64 num_units, const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        using ResultColumnType = typename ResultDataType::ColumnType;
        using ResultFieldType = typename ResultDataType::FieldType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto * col_to = assert_cast<ResultColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        const Int64 scale_time = DecimalUtils::scaleMultiplier<DateTime64>(scale);
        const Int64 scale_interval = scaleFromInterval<unit>();

        /// In case if we have a difference between time arguments and Interval, we need to calculate the difference between them
        /// to get the right precision for the result.
        const Int64 scale_diff = (scale_interval > scale_time) ? (scale_interval / scale_time) : (scale_time / scale_interval);

        if (origin_column.column == nullptr)
        {
            if (scale_time > scale_interval && scale_interval != 1)
            {
                for (size_t i = 0; i != size; ++i)
                {
                    /// If we have a time argument that has bigger scale than the interval can contain and interval is not default, we need
                    /// to return a value with bigger precision and thus we should multiply result on the scale difference.
                    result_data[i] = 0;
                    result_data[i] += static_cast<ResultFieldType>(ToStartOfInterval<unit>::execute(time_data[i], num_units, time_zone, scale_time)) * scale_diff;
                }
            }
            else
            {
                for (size_t i = 0; i != size; ++i)
                {
                    auto res = static_cast<ResultFieldType>(ToStartOfInterval<unit>::execute(time_data[i], num_units, time_zone, scale_time));
                    result_data[i] = res;
                }
            }
        }
        else
        {
            UInt64 origin = origin_column.column->get64(0);

            for (size_t i = 0; i != size; ++i)
            {
                Int64 end_time = time_data[i];
                if (origin > static_cast<size_t>(end_time))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The origin must be before the end date / date with time");

                /// The trick to calculate the interval starting from an offset is to
                /// 1. subtract the offset,
                /// 2. perform the calculation, and
                /// 3. add the offset to the result.

                static constexpr size_t SECONDS_PER_DAY = 86'400;

                result_data[i] = 0;
                Int64 delta = end_time - origin;
                Int64 res = 0;
                if (unit == IntervalKind::Day || unit == IntervalKind::Week)
                {
                    delta *= SECONDS_PER_DAY * scale_time;
                    if (unit == IntervalKind::Week)
                        res = static_cast<DataTypeDateTime::FieldType>(ToStartOfInterval<IntervalKind::Day>::execute(delta, num_units * 7, time_zone, scale_time));
                    else
                        res = static_cast<DataTypeDateTime::FieldType>(ToStartOfInterval<unit>::execute(delta, num_units, time_zone, scale_time));
                }
                else if (unit == IntervalKind::Month || unit == IntervalKind::Quarter)
                {
                    Int64 months = time_zone.toMonth((delta + origin) * SECONDS_PER_DAY) - time_zone.toMonth(origin * SECONDS_PER_DAY);
                    num_units = unit == IntervalKind::Month ? num_units : num_units * 3;
                    Int64 month_multiplier = (months / num_units) * num_units;
                    auto a = time_zone.addMonths(time_zone.toDate(origin * SECONDS_PER_DAY), month_multiplier);
                    result_data[i] += a / SECONDS_PER_DAY;
                }
                else if (unit == IntervalKind::Year)
                {
                    Int64 years = time_zone.toYear((delta + origin) * SECONDS_PER_DAY) - time_zone.toYear(origin * SECONDS_PER_DAY);
                    Int64 year_multiplier = (years / num_units) * num_units;
                    auto a = time_zone.addYears(time_zone.toDate(origin * SECONDS_PER_DAY), year_multiplier);
                    result_data[i] += a / SECONDS_PER_DAY;
                }
                else
                {
                    res = static_cast<ResultFieldType>(ToStartOfInterval<unit>::execute(delta, num_units, time_zone, scale_time));
                }
                
                
                if (unit == IntervalKind::Week)
                {
                    /// For such intervals, ToStartOfInterval<unit>::execute() returns days
                    if (isDate(result_type))
                        result_data[i] += origin + res / SECONDS_PER_DAY;
                    else if (isDateTime(result_type))
                        result_data[i] += origin + res * SECONDS_PER_DAY;
                    else if (isDateTime64(result_type))
                        result_data[i] += origin + (res * SECONDS_PER_DAY * scale_time);
                }
                else if (unit != IntervalKind::Month && unit != IntervalKind::Quarter && unit != IntervalKind::Year)
                {
                    /// ToStartOfInterval<unit>::execute() returns seconds

                    if (isDate(result_type))
                        res = res / SECONDS_PER_DAY;

                    if (scale_interval == 1)
                    {
                        /// Interval has default scale, i.e. Year - Second

                        if (scale_time % 1000 != 0 && scale_time >= 1000)
                            /// The arguments are DateTime64 with precision like 4,5,7,8. Here res has right precision and origin doesn't.
                            result_data[i] += (origin + res / scale_time) * scale_time;
                        else if (scale_time == 100)
                            /// The arguments are DateTime64 with precision 2. Here origin has right precision and res doesn't
                            result_data[i] += (origin + res * scale_time);
                        else
                            /// Precision of DateTime64 is 1, 3, 6, 9, e.g. has right precision in res and origin.
                            result_data[i] += (origin + res * scale_diff);
                    }
                    else
                    {
                        /// Interval has some specific scale (3,6,9), i.e. Millisecond - Nanosecond

                        if (scale_interval < scale_time)
                            /// If we have a time argument that has bigger scale than the interval can contain, we need
                            /// to return a value with bigger precision and thus we should multiply result on the scale difference.
                            result_data[i] += origin + res * scale_diff;
                        else
                            /// The other case: interval has bigger scale than the interval or they have the same scale, so res has the right precision and origin doesn't
                            result_data[i] += (origin + res / scale_diff) * scale_diff;
                    }
                }
            }
        }
        return result_col;
    }
};

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
