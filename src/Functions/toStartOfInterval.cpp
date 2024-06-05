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


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }
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

        enum class ResultType : uint8_t
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
            if (!isString(type_arg3))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 3rd argument of function {}, expected a constant timezone string",
                    type_arg3->getName(), getName());
            if (value_is_date && result_type == ResultType::Date) /// weird why this is && instead of || but too afraid to change it
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The timezone argument of function {} with interval type {} is allowed only when the 1st argument has type DateTime or DateTimt64",
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
        else
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());
        }

        switch (result_type)
        {
            case ResultType::Date:
                return std::make_shared<DataTypeDate>();
            case ResultType::DateTime:
                return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
            case ResultType::DateTime64:
            {
                UInt32 scale = 0;
                if (interval_type->getKind() == IntervalKind::Kind::Nanosecond)
                    scale = 9;
                else if (interval_type->getKind() == IntervalKind::Kind::Microsecond)
                    scale = 6;
                else if (interval_type->getKind() == IntervalKind::Kind::Millisecond)
                    scale = 3;

                return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
            }
        }

        std::unreachable();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /* input_rows_count */) const override
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const auto & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        auto result_column = dispatchForTimeColumn(time_column, interval_column, result_type, time_zone);
        return result_column;
    }

private:
    ColumnPtr dispatchForTimeColumn(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & time_column_type = *time_column.type.get();
        const auto & time_column_col = *time_column.column.get();

        if (isDateTime64(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(&time_column_col);
            auto scale = assert_cast<const DataTypeDateTime64 &>(time_column_type).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64 &>(time_column_type), *time_column_vec, interval_column, result_type, time_zone, scale);
        }
        else if (isDateTime(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(&time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime &>(time_column_type), *time_column_vec, interval_column, result_type, time_zone);
        }
        else if (isDate(time_column_type))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(&time_column_col);
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate &>(time_column_type), *time_column_vec, interval_column, result_type, time_zone);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for 1st argument of function {}, expected a Date, DateTime or DateTime64", getName());
    }

    template <typename TimeDataType, typename TimeColumnType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column,
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
            case IntervalKind::Kind::Nanosecond:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime64, IntervalKind::Kind::Nanosecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Microsecond:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime64, IntervalKind::Kind::Microsecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Millisecond:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime64, IntervalKind::Kind::Millisecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Second:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime, IntervalKind::Kind::Second>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Minute:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime, IntervalKind::Kind::Minute>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Hour:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime, IntervalKind::Kind::Hour>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Day:
                return execute<TimeDataType, TimeColumnType, DataTypeDateTime, IntervalKind::Kind::Day>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Week:
                return execute<TimeDataType, TimeColumnType, DataTypeDate, IntervalKind::Kind::Week>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Month:
                return execute<TimeDataType, TimeColumnType, DataTypeDate, IntervalKind::Kind::Month>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Quarter:
                return execute<TimeDataType, TimeColumnType, DataTypeDate, IntervalKind::Kind::Quarter>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Kind::Year:
                return execute<TimeDataType, TimeColumnType, DataTypeDate, IntervalKind::Kind::Year>(time_data_type, time_column, num_units, result_type, time_zone, scale);
        }

        std::unreachable();
    }

    template <typename TimeDataType, typename TimeColumnType, typename ResultDataType, IntervalKind::Kind unit>
    ColumnPtr execute(
        const TimeDataType &, const TimeColumnType & time_column_type, Int64 num_units,
        const DataTypePtr & result_type, const DateLUTImpl & time_zone, UInt16 scale) const
    {
        using ResultColumnType = typename ResultDataType::ColumnType;
        using ResultFieldType = typename ResultDataType::FieldType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto * col_to = assert_cast<ResultColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        Int64 scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

        for (size_t i = 0; i != size; ++i)
            result_data[i] = static_cast<ResultFieldType>(ToStartOfInterval<unit>::execute(time_data[i], num_units, time_zone, scale_multiplier));

        return result_col;
    }
};

REGISTER_FUNCTION(ToStartOfInterval)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
