#include <common/DateLUTImpl.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
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
        static constexpr auto name = function_name;

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
    struct Transform<IntervalKind::Quarter>
    {
        static constexpr auto name = function_name;

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
    struct Transform<IntervalKind::Month>
    {
        static constexpr auto name = function_name;

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
    struct Transform<IntervalKind::Week>
    {
        static constexpr auto name = function_name;

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
    struct Transform<IntervalKind::Day>
    {
        static constexpr auto name = function_name;

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
    struct Transform<IntervalKind::Hour>
    {
        static constexpr auto name = function_name;

        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 hours, const DateLUTImpl & time_zone) { return time_zone.toStartOfHourInterval(t, hours); }
    };

    template <>
    struct Transform<IntervalKind::Minute>
    {
        static constexpr auto name = function_name;

        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 minutes, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfMinuteInterval(t, minutes);
        }
    };

    template <>
    struct Transform<IntervalKind::Second>
    {
        static constexpr auto name = function_name;

        static UInt32 execute(UInt16, UInt64, const DateLUTImpl &) { return dateIsNotSupported(function_name); }

        static UInt32 execute(UInt32 t, UInt64 seconds, const DateLUTImpl & time_zone)
        {
            return time_zone.toStartOfSecondInterval(t, seconds);
        }
    };


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
            result_type_is_date = (interval_type->getKind() == IntervalKind::Year)
                || (interval_type->getKind() == IntervalKind::Quarter) || (interval_type->getKind() == IntervalKind::Month)
                || (interval_type->getKind() == IntervalKind::Week);
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
                    "The timezone argument of function " + getName() + " with interval type " + interval_type->getKind().toString()
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /* input_rows_count */) const override
    {
        const auto & time_column = arguments[0];
        const auto & interval_column = arguments[1];
        const DateLUTImpl & time_zone = extractTimeZoneFromFunctionArguments(arguments, 2, 0);
        auto result_column = dispatchForColumns(time_column, interval_column, time_zone);
        return result_column;
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
        const ColumnWithTypeAndName & time_column, const ColumnWithTypeAndName & interval_column, const DateLUTImpl & time_zone) const
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
        const FromDataType & from, const ColumnType & time_column, const ColumnWithTypeAndName & interval_column, const DateLUTImpl & time_zone) const
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
            case IntervalKind::Second:
                return execute<FromDataType, UInt32, IntervalKind::Second>(from, time_column, num_units, time_zone);
            case IntervalKind::Minute:
                return execute<FromDataType, UInt32, IntervalKind::Minute>(from, time_column, num_units, time_zone);
            case IntervalKind::Hour:
                return execute<FromDataType, UInt32, IntervalKind::Hour>(from, time_column, num_units, time_zone);
            case IntervalKind::Day:
                return execute<FromDataType, UInt32, IntervalKind::Day>(from, time_column, num_units, time_zone);
            case IntervalKind::Week:
                return execute<FromDataType, UInt16, IntervalKind::Week>(from, time_column, num_units, time_zone);
            case IntervalKind::Month:
                return execute<FromDataType, UInt16, IntervalKind::Month>(from, time_column, num_units, time_zone);
            case IntervalKind::Quarter:
                return execute<FromDataType, UInt16, IntervalKind::Quarter>(from, time_column, num_units, time_zone);
            case IntervalKind::Year:
                return execute<FromDataType, UInt16, IntervalKind::Year>(from, time_column, num_units, time_zone);
        }

        __builtin_unreachable();
    }


    template <typename FromDataType, typename ToType, IntervalKind::Kind unit, typename ColumnType>
    ColumnPtr execute(const FromDataType & from_datatype, const ColumnType & time_column, UInt64 num_units, const DateLUTImpl & time_zone) const
    {
        const auto & time_data = time_column.getData();
        size_t size = time_column.size();
        auto result = ColumnVector<ToType>::create();
        auto & result_data = result->getData();
        result_data.resize(size);

        if constexpr (std::is_same_v<FromDataType, DataTypeDateTime64>)
        {
            const auto transform = TransformDateTime64<Transform<unit>>{from_datatype.getScale()};
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

}

void registerFunctionToStartOfInterval(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToStartOfInterval>();
}

}
