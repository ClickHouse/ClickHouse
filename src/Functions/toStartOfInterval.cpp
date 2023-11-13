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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool first_argument_is_date = false;
        auto check_first_argument = [&]
        {
            if (!isDate(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "Should be a date or a date with time", arguments[0].type->getName(), getName());
            first_argument_is_date = isDate(arguments[0].type);
        };

        const DataTypeInterval * interval_type = nullptr;
        bool result_type_is_date = false;
        bool result_type_is_datetime = false;
        bool result_type_is_datetime_64 = false;
        auto check_interval_argument = [&]
        {
            interval_type = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());
            if (!interval_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "Should be an interval of time", arguments[1].type->getName(), getName());
            switch (interval_type->getKind()) // NOLINT(bugprone-switch-missing-default-case)
            {
                case IntervalKind::Nanosecond:
                case IntervalKind::Microsecond:
                case IntervalKind::Millisecond:
                    result_type_is_datetime_64 = true;
                    break;
                case IntervalKind::Second:
                case IntervalKind::Minute:
                case IntervalKind::Hour:
                case IntervalKind::Day:
                    result_type_is_datetime = true;
                    break;
                case IntervalKind::Week:
                case IntervalKind::Month:
                case IntervalKind::Quarter:
                case IntervalKind::Year:
                    result_type_is_date = true;
                    break;
            }
        };

        auto check_timezone_argument = [&]
        {
            if (!isString(arguments[2].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}. "
                    "This argument is optional and must be a constant string with timezone name",
                    arguments[2].type->getName(), getName());
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                getName(), arguments.size());
        }

        if (result_type_is_date)
            return std::make_shared<DataTypeDate>();
        else if (result_type_is_datetime)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
        else if (result_type_is_datetime_64)
        {
            auto scale = 0;

            if (interval_type->getKind() == IntervalKind::Nanosecond)
                scale = 9;
            else if (interval_type->getKind() == IntervalKind::Microsecond)
                scale = 6;
            else if (interval_type->getKind() == IntervalKind::Millisecond)
                scale = 3;

            return std::make_shared<DataTypeDateTime64>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 2, 0, false));
        }

        UNREACHABLE();
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
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const DataTypePtr & result_type, const DateLUTImpl & time_zone) const
    {
        const auto & from_datatype = *time_column.type.get();

        if (isDateTime64(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime64>(time_column.column.get());
            auto scale = assert_cast<const DataTypeDateTime64 &>(from_datatype).getScale();

            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64 &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone, scale);
        }
        if (isDateTime(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDateTime>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (isDate(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        if (isDate32(from_datatype))
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnDate32>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate32 &>(from_datatype), *time_column_vec, interval_column, result_type, time_zone);
        }
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column for first argument of function {}. Must contain dates or dates with time", getName());
    }

    template <typename TimeColumnType, typename TimeDataType>
    ColumnPtr dispatchForIntervalColumn(
        const TimeDataType & time_data_type, const TimeColumnType & time_column, const ColumnWithTypeAndName & interval_column,
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
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Nanosecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Microsecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Microsecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Millisecond:
                return execute<TimeDataType, DataTypeDateTime64, IntervalKind::Millisecond>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Second:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Second>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Minute:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Minute>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Hour:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Hour>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Day:
                return execute<TimeDataType, DataTypeDateTime, IntervalKind::Day>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Week:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Week>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Month:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Month>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Quarter:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Quarter>(time_data_type, time_column, num_units, result_type, time_zone, scale);
            case IntervalKind::Year:
                return execute<TimeDataType, DataTypeDate, IntervalKind::Year>(time_data_type, time_column, num_units, result_type, time_zone, scale);
        }

        UNREACHABLE();
    }

    template <typename TimeDataType, typename ToDataType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const TimeDataType &, const ColumnType & time_column_type, Int64 num_units, const DataTypePtr & result_type, const DateLUTImpl & time_zone, const UInt16 scale) const
    {
        using ToColumnType = typename ToDataType::ColumnType;
        using ToFieldType = typename ToDataType::FieldType;

        const auto & time_data = time_column_type.getData();
        size_t size = time_data.size();

        auto result_col = result_type->createColumn();
        auto *col_to = assert_cast<ToColumnType *>(result_col.get());
        auto & result_data = col_to->getData();
        result_data.resize(size);

        Int64 scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64>(scale);

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
