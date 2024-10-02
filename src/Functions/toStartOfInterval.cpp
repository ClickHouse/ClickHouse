#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Common/DateLUTImpl.h>
#include <Common/IntervalKind.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
            if (!isDateOrDate32(type_arg1) && !isDateTime(type_arg1) && !isDateTime64(type_arg1))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 1st argument of function {}, expected a Date, Date32, DateTime or DateTime64",
                    type_arg1->getName(), getName());
            value_is_date = isDate(type_arg1);
        };

        const DataTypeInterval * interval_type = nullptr;

        enum class ResultType : uint8_t
        {
            Date,
            Date32,
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
                case IntervalKind::Kind::Nanosecond:
                case IntervalKind::Kind::Microsecond:
                case IntervalKind::Kind::Millisecond:
                    result_type = ResultType::DateTime64;
                    break;
                case IntervalKind::Kind::Second:
                case IntervalKind::Kind::Minute:
                case IntervalKind::Kind::Hour:
                case IntervalKind::Kind::Day: /// weird why Day leads to DateTime but too afraid to change it
                    result_type = ResultType::DateTime;
                    break;
                case IntervalKind::Kind::Week:
                case IntervalKind::Kind::Month:
                case IntervalKind::Kind::Quarter:
                case IntervalKind::Kind::Year:
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
            else if (isDateOrDate32OrDateTimeOrDateTime64(type_arg3))
            {
                overload = Overload::Origin;
                const DataTypePtr & type_arg1 = arguments[0].type;
                if (isDate(type_arg1) && isDate(type_arg3))
                    result_type = ResultType::Date;
                else if (isDate32(type_arg1) && isDate32(type_arg3))
                    result_type = ResultType::Date32;
                else if (isDateTime(type_arg1) && isDateTime(type_arg3))
                    result_type = ResultType::DateTime;
                else if (isDateTime64(type_arg1) && isDateTime64(type_arg3))
                    result_type = ResultType::DateTime64;
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
                "Number of arguments for function {} doesn't match: passed {}, must be 2, 3 or 4",
                getName(), arguments.size());
        }

        switch (result_type)
        {
            case ResultType::Date:
                return std::make_shared<DataTypeDate>();
            case ResultType::Date32:
                return std::make_shared<DataTypeDate32>();
            case ResultType::DateTime:
            {
                const size_t time_zone_arg_num = (overload == Overload::Default) ? 2 : 3;
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, time_zone_arg_num, 0, false));
            }
            case ResultType::DateTime64:
            {
                UInt32 scale = 0;
                if (isDateTime64(arguments[0].type) && overload == Overload::Origin)
                {
                    scale = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type.get()).getScale();
                    if (assert_cast<const DataTypeDateTime64 &>(*arguments[2].type.get()).getScale() != scale)
                        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Datetime argument and origin argument for function {} must have the same scale", getName());
                }
                if (interval_type->getKind() == IntervalKind::Kind::Nanosecond)
                    scale = 9;
                else if (interval_type->getKind() == IntervalKind::Kind::Microsecond)
                    scale = 6;
                else if (interval_type->getKind() == IntervalKind::Kind::Millisecond)
                    scale = 3;

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

        const DateLUTImpl * time_zone_tmp;

        if (isDateTimeOrDateTime64(time_column.type) || isDateTimeOrDateTime64(result_type))
        {
            const size_t time_zone_arg_num = (overload == Overload::Default) ? 2 : 3;
            time_zone_tmp = &extractTimeZoneFromFunctionArguments(arguments, time_zone_arg_num, 0);
        }
        else /// As we convert date to datetime and perform calculation, we don't need to take the timezone into account, so we set it to default
            time_zone_tmp = &DateLUT::instance("UTC");

        const DateLUTImpl & time_zone = *time_zone_tmp;

        ColumnPtr result_column;
        if (isDate(result_type))
            result_column = dispatchForTimeColumn<DataTypeDate>(time_column, interval_column, origin_column, result_type, time_zone);
        else if (isDate32(result_type))
            result_column = dispatchForTimeColumn<DataTypeDate32>(time_column, interval_column, origin_column, result_type, time_zone);
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
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(&time_column_col);

            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType, DataTypeDate, ColumnDate>(assert_cast<const DataTypeDate &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDate32(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate32>(&time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType, DataTypeDate32, ColumnDate32>(assert_cast<const DataTypeDate32 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDateTime(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(&time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType, DataTypeDateTime, ColumnDateTime>(assert_cast<const DataTypeDateTime &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDateTime64(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(&time_column_col);
            auto scale = assert_cast<const DataTypeDateTime64 &>(time_column_type).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn<ReturnType, DataTypeDateTime64, ColumnDateTime64>(assert_cast<const DataTypeDateTime64 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone, scale);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for 1st argument of function {}, expected a Date, Date32, DateTime or DateTime64", getName());
    }

    template <typename ReturnType, typename TimeDataType, typename TimeColumnType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column, const ColumnWithTypeAndName & origin_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, UInt16 scale = 1) const
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for 2nd argument of function {}, must be a time interval", getName());

        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Kind::Nanosecond:
            case IntervalKind::Kind::Microsecond:
            case IntervalKind::Kind::Millisecond:
                if (isDateOrDate32(time_data_type) || isDateTime(time_data_type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal interval kind for argument data type {}", isDate(time_data_type) ? "Date" : "DateTime");
                break;
            case IntervalKind::Kind::Second:
            case IntervalKind::Kind::Minute:
            case IntervalKind::Kind::Hour:
                if (isDateOrDate32(time_data_type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal interval kind for argument data type Date");
                break;
            default:
                break;
        }

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for 2nd argument of function {}, must be a const time interval", getName());

        const Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Value for 2nd argument of function {} must be positive", getName());

        switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
        {
            case IntervalKind::Kind::Nanosecond:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Nanosecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Microsecond:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Microsecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Millisecond:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Millisecond>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Second:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Second>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Minute:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Minute>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Hour:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Hour>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Day:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Day>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Week:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Week>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Month:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Month>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Quarter:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Quarter>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
            case IntervalKind::Kind::Year:
                return execute<ReturnType, TimeDataType, TimeColumnType, IntervalKind::Kind::Year>(time_data_type, time_column, num_units, origin_column, result_type, time_zone, scale);
        }

        std::unreachable();
    }

    template <typename ResultDataType, typename TimeDataType, typename TimeColumnType, IntervalKind::Kind unit>
    ColumnPtr execute(const TimeDataType &, const TimeColumnType & time_column_type, Int64 num_units, const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        using ResultColumnType = typename ResultDataType::ColumnType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto * col_to = assert_cast<ResultColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        Int64 scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

        if (origin_column.column) // Overload: Origin
        {
            const bool is_small_interval = (unit == IntervalKind::Kind::Nanosecond || unit == IntervalKind::Kind::Microsecond || unit == IntervalKind::Kind::Millisecond);
            const bool is_result_date = isDateOrDate32(result_type);

            Int64 result_scale = scale_multiplier;
            Int64 origin_scale = 1;

            if (isDateTime64(result_type)) /// We have origin scale only in case if arguments are DateTime64.
                origin_scale = assert_cast<const DataTypeDateTime64 &>(*origin_column.type).getScaleMultiplier();
            else if (!is_small_interval) /// In case of large interval and arguments are not DateTime64, we should not have scale in result.
                result_scale = 1;

            if (is_small_interval)
                result_scale = assert_cast<const DataTypeDateTime64 &>(*result_type).getScaleMultiplier();

            /// In case if we have a difference between time arguments and Interval, we need to calculate the difference between them
            /// to get the right precision for the result. In case of large intervals, we should not have scale difference.
            Int64 scale_diff = is_small_interval ? std::max(result_scale / origin_scale, origin_scale / result_scale) : 1;

            static constexpr Int64 SECONDS_PER_DAY = 86'400;

            UInt64 origin = origin_column.column->get64(0);
            for (size_t i = 0; i != size; ++i)
            {
                UInt64 time_arg = time_data[i];
                if (origin > static_cast<size_t>(time_arg))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The origin must be before the end date / date with time");

                if (is_result_date) /// All internal calculations of ToStartOfInterval<...> expect arguments to be seconds or milli-, micro-, nanoseconds.
                {
                    time_arg *= SECONDS_PER_DAY;
                    origin *= SECONDS_PER_DAY;
                }

                Int64 offset = ToStartOfInterval<unit>::execute(time_arg - origin, num_units, time_zone, result_scale, origin);

                /// In case if arguments are DateTime64 with large interval, we should apply scale on it.
                offset *= (!is_small_interval) ? result_scale : 1;

                if (is_result_date) /// Convert back to date after calculations.
                {
                    offset /= SECONDS_PER_DAY;
                    origin /= SECONDS_PER_DAY;
                }

                result_data[i] = 0;
                result_data[i] += (result_scale < origin_scale) ? (origin + offset) / scale_diff : (origin + offset) * scale_diff;
            }
        }
        else // Overload: Default
        {
            for (size_t i = 0; i != size; ++i)
                result_data[i] = static_cast<typename ResultDataType::FieldType>(ToStartOfInterval<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier));
        }

        return result_col;
    }
};

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
    factory.registerAlias("time_bucket", "toStartOfInterval", FunctionFactory::Case::Insensitive);
    factory.registerAlias("date_bin", "toStartOfInterval", FunctionFactory::Case::Insensitive);
}

}
