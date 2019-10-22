#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
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
    static constexpr auto function_name = "toStartOfInterval";

    template <DataTypeInterval::Kind unit>
    struct Transform;

    template <>
    struct Transform<DataTypeInterval::Year>
    {
        static UInt16 execute(UInt16 d, UInt64 years, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfYearInterval(DayNum(d), years);
        }

        static UInt16 execute(UInt32 t, UInt64 years, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfYearInterval(time_zone.toDayNum(t), years);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Quarter>
    {
        static UInt16 execute(UInt16 d, UInt64 quarters, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfQuarterInterval(DayNum(d), quarters);
        }

        static UInt16 execute(UInt32 t, UInt64 quarters, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfQuarterInterval(time_zone.toDayNum(t), quarters);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Month>
    {
        static UInt16 execute(UInt16 d, UInt64 months, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfMonthInterval(DayNum(d), months);
        }

        static UInt16 execute(UInt32 t, UInt64 months, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfMonthInterval(time_zone.toDayNum(t), months);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Week>
    {
        static UInt16 execute(UInt16 d, UInt64 weeks, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfWeekInterval(DayNum(d), weeks);
        }

        static UInt16 execute(UInt32 t, UInt64 weeks, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfWeekInterval(time_zone.toDayNum(t), weeks);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Day>
    {
        static UInt32 execute(UInt16 d, UInt64 days, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfDayInterval(DayNum(d), days);
        }

        static UInt32 execute(UInt32 t, UInt64 days, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfDayInterval(time_zone.toDayNum(t), days);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Hour>
    {
        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 hours, const DateLUTImpl & time_zone) { return time_zone.toStartOfHourInterval(t, hours); }
    };

    template <>
    struct Transform<DataTypeInterval::Minute>
    {
        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 minutes, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfMinuteInterval(t, minutes);
        }
    };

    template <>
    struct Transform<DataTypeInterval::Second>
    {
        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 seconds, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfSecondInterval(t, seconds);
        }
    };

    template <typename Transform>
    class DateTime64TransformWrapper
    {
    public:
        DateTime64TransformWrapper(UInt32 scale_)
            : scale_multiplier(decimalScaleMultiplier<DateTime64::NativeType>(scale_)),
              fractional_divider(decimalFractionalDivider<DateTime64>(scale_))
        {}

        UInt32 execute(DateTime64 t, UInt64 v, const DateLUTImpl & time_zone) const
        {
            const auto components = decimalSplitWithScaleMultiplier(t, scale_multiplier);
            return Transform::execute(static_cast<UInt32>(components.whole), v, time_zone);
        }
    private:
        UInt32 scale_multiplier = 1;
        UInt32 fractional_divider = 1;
    };
}


class FunctionToStartOfInterval : public IFunction
{
public:
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionToStartOfInterval>(); }

    static constexpr auto name = function_name;
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool first_argument_is_date = false;
        auto check_first_argument = [&]
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            first_argument_is_date = isDate(arguments[0].type);
        };

        const DataTypeInterval * interval_type = nullptr;
        bool result_type_is_date = false;
        auto check_interval_argument = [&]
        {
            interval_type = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());
            if (!interval_type)
                throw Exception(
                    "Illegal type " + arguments[1].type->getName() + " of argument of function " + getName()
                        + ". Should be an interval of time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            result_type_is_date = (interval_type->getKind() == DataTypeInterval::Year)
                || (interval_type->getKind() == DataTypeInterval::Quarter) || (interval_type->getKind() == DataTypeInterval::Month)
                || (interval_type->getKind() == DataTypeInterval::Week);
        };

        auto check_timezone_argument = [&]
        {
            if (!WhichDataType(arguments[2].type).isString())
                throw Exception(
                    "Illegal type " + arguments[2].type->getName() + " of argument of function " + getName()
                        + ". This argument is optional and must be a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (first_argument_is_date && result_type_is_date)
                throw Exception(
                    "The timezone argument of function " + getName() + " with interval type " + interval_type->kindToString()
                        + " is allowed only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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
        else
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 2, 0));
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /* input_rows_count */) override
    {
        const auto & time_column = block.getByPosition(arguments[0]);
        const auto & interval_column = block.getByPosition(arguments[1]);
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(block, arguments, 2, 0);
        auto result_column = dispatchForColumns(time_column, interval_column, time_zone);
        block.getByPosition(result).column = std::move(result_column);
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { true, true, true };
    }

private:
    ColumnPtr dispatchForColumns(
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const DateLUTImpl & time_zone)
    {
        const auto & from_datatype = *time_column.type.get();
        const auto which_type = WhichDataType(from_datatype);
        if (which_type.isDateTime())
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt32>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime&>(from_datatype), *time_column_vec, interval_column, time_zone);
        }
        if (which_type.isDate())
        {
            const auto * time_column_vec = checkAndGetColumn<ColumnUInt16>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDate&>(from_datatype), *time_column_vec, interval_column, time_zone);
        }
        if (which_type.isDateTime64())
        {
            const auto * time_column_vec = checkAndGetColumn<DataTypeDateTime64::ColumnType>(time_column.column.get());
            if (time_column_vec)
                return dispatchForIntervalColumn(assert_cast<const DataTypeDateTime64&>(from_datatype), *time_column_vec, interval_column, time_zone);
        }
        throw Exception(
            "Illegal column for first argument of function " + getName() + ". Must contain dates or dates with time",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <typename ColumnType, typename FromDataType>
    ColumnPtr dispatchForIntervalColumn(
        const FromDataType & from, const ColumnType & time_column, const ColumnWithTypeAndName & interval_column, const DateLUTImpl & time_zone)
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
            case DataTypeInterval::Second:
                return execute<FromDataType, UInt32, DataTypeInterval::Second>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Minute:
                return execute<FromDataType, UInt32, DataTypeInterval::Minute>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Hour:
                return execute<FromDataType, UInt32, DataTypeInterval::Hour>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Day:
                return execute<FromDataType, UInt32, DataTypeInterval::Day>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Week:
                return execute<FromDataType, UInt16, DataTypeInterval::Week>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Month:
                return execute<FromDataType, UInt16, DataTypeInterval::Month>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Quarter:
                return execute<FromDataType, UInt16, DataTypeInterval::Quarter>(from, time_column, num_units, time_zone);
            case DataTypeInterval::Year:
                return execute<FromDataType, UInt16, DataTypeInterval::Year>(from, time_column, num_units, time_zone);
        }

        __builtin_unreachable();
    }

    template <typename FromDataType, typename ToType, DataTypeInterval::Kind unit, typename ColumnType>
    ColumnPtr execute(const FromDataType & from_datatype, const ColumnType & time_column, UInt64 num_units, const DateLUTImpl & time_zone)
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        auto result = ColumnVector<ToType>::create();
        auto & result_data = result->getData();
        result_data.resize(size);

        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
        {
            const auto transform = DateTime64TransformWrapper<Transform<unit>>{from_datatype.getScale()};
            for (size_t i = 0; i != size; ++i)
                result_data[i] = transform.execute(time_data[i], num_units, time_zone);
        }
        else
        {
            for (size_t i = 0; i != size; ++i)
                result_data[i] = Transform<unit>::execute(time_data[i], num_units, time_zone);
        }
        return result;
    }
};


void registerFunctionToStartOfInterval(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
