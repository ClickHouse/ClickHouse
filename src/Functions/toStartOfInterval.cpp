#include <base/arithmeticOverflow.h>
#include <Common/DateLUTImpl.h>
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
}


namespace
{

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

        const size_t time_zone_arg_num = (arguments.size() == 2 || (arguments.size() == 3 && isString(arguments[2].type))) ? 2 : 3;
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
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column_col);
            auto scale = assert_cast<const DataTypeDateTime64 &>(time_column_type).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64 &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone, scale);
        }
        else if (isDateTime(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDate(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate &>(time_column_type), *time_column_vec, interval_column, origin_column, result_type, time_zone);
        }
        else if (isDate32(time_column_type))
        {
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
    ColumnPtr execute(const TimeDataType &, const ColumnType & time_column_type, Int64 num_units, [[maybe_unused]] const ColumnWithTypeAndName & origin_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale) const
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

        /// TODO: This part is missing. origin_column is either {} (<-- to check, you could do `origin_column.column == nullptr`) or not {}
        ///       In the former case, we can execute below existing code.
        ///       In the latter case, we need to read the actual origin value. As per `getArgumentsThatAreAlwaysConstant()` (see above), we
        ///       can be sure that origin_column is a `ColumnConst`. The second assumption we can reasonable make is that it has the same
        ///       type (Date/Date32/DateTime/DateTime64) as the time column (1st argument). Since the method we are in is already
        ///       templatized on the data type of the time column, we can use `checkAndGetColumnConst<ColumnType>(...)` to cast
        ///       `origin_column.column` to a const column and then read the (const) value from it, and proceed with the calculations.

        for (size_t i = 0; i != size; ++i)
            result_data[i] = static_cast<ToFieldType>(ToStartOfInterval<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier));

        return result_col;
    }
};

}

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
