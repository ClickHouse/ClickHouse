#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

namespace
{

enum class IcebergDateTransform : uint8_t
{
    Year,
    Month,
    Day,
    Hour,
};

Int64 floorDivide(Int64 numerator, Int64 denominator)
{
    Int64 quotient = numerator / denominator;
    if (numerator % denominator != 0 && (numerator < 0) != (denominator < 0))
        --quotient;
    return quotient;
}

Int32 narrowToInt32(Int64 value, const char * function_name)
{
    if (value < std::numeric_limits<Int32>::min() || value > std::numeric_limits<Int32>::max())
        throw Exception(
            ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
            "Result {} of function {} does not fit into the Iceberg `int` partition value type",
            value,
            function_name);
    return static_cast<Int32>(value);
}

template <IcebergDateTransform transform>
class FunctionIcebergDateTransform final : public IFunction
{
public:
    static constexpr const char * name = []
    {
        if constexpr (transform == IcebergDateTransform::Year)
            return "icebergYear";
        else if constexpr (transform == IcebergDateTransform::Month)
            return "icebergMonth";
        else if constexpr (transform == IcebergDateTransform::Day)
            return "icebergDay";
        else
            return "icebergHour";
    }();

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIcebergDateTransform>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    bool hasInformationAboutMonotonicity() const override { return true; }
    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return {.is_monotonic = true, .is_always_monotonic = true};
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments of function {}: expected 1", name);

        WhichDataType which(arguments[0]);
        if constexpr (transform == IcebergDateTransform::Hour)
        {
            if (!which.isDateTime() && !which.isDateTime64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument of function {} must be DateTime or DateTime64, got {}",
                    name,
                    arguments[0]->getName());
        }
        else if (!which.isDateTime() && !which.isDateTime64() && !which.isDate() && !which.isDate32())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be Date, Date32, DateTime or DateTime64, got {}",
                name,
                arguments[0]->getName());
        }

        return std::make_shared<DataTypeInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto result_column = ColumnInt32::create(input_rows_count);
        auto & result_data = result_column->getData();

        const auto & argument = arguments[0];
        WhichDataType which(argument.type);

        if (which.isDateTime())
        {
            const auto & data = assert_cast<const ColumnDateTime &>(*argument.column).getData();
            fillFromTicks(result_data, input_rows_count, [&](size_t i) { return static_cast<Int64>(data[i]); }, 1);
        }
        else if (which.isDateTime64())
        {
            const auto & column = assert_cast<const ColumnDateTime64 &>(*argument.column);
            const Int64 ticks_per_second = DecimalUtils::scaleMultiplier<Int64>(column.getScale());
            const auto & data = column.getData();
            fillFromTicks(
                result_data, input_rows_count, [&](size_t i) { return static_cast<Int64>(data[i].value); }, ticks_per_second);
        }
        else if constexpr (transform != IcebergDateTransform::Hour)
        {
            if (which.isDate())
            {
                const auto & data = assert_cast<const ColumnDate &>(*argument.column).getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = fromDayNum(static_cast<Int64>(data[i]));
            }
            else if (which.isDate32())
            {
                const auto & data = assert_cast<const ColumnDate32 &>(*argument.column).getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    result_data[i] = fromDayNum(static_cast<Int64>(data[i]));
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", argument.type->getName(), name);
        }
        else
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", argument.type->getName(), name);

        return result_column;
    }

private:
    template <typename GetTicks>
    static void fillFromTicks(
        ColumnInt32::Container & result_data, size_t input_rows_count, GetTicks && get_ticks, Int64 ticks_per_second)
    {
        if constexpr (transform == IcebergDateTransform::Hour)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                result_data[i] = narrowToInt32(floorDivide(get_ticks(i), ticks_per_second * 3600), name);
        }
        else
        {
            for (size_t i = 0; i < input_rows_count; ++i)
                result_data[i] = fromDayNum(floorDivide(get_ticks(i), ticks_per_second * 86400));
        }
    }

    static Int32 fromDayNum(Int64 day)
    {
        if constexpr (transform == IcebergDateTransform::Day)
            return narrowToInt32(day, name);
        else
        {
            const auto & utc = DateLUT::instance("UTC");
            const auto day_num = ExtendedDayNum(narrowToInt32(day, name));
            if constexpr (transform == IcebergDateTransform::Year)
                return utc.toYearSinceEpoch(day_num);
            else
                return utc.toMonthNumSinceEpoch(day_num);
        }
    }
};

}

REGISTER_FUNCTION(IcebergDateTransforms)
{
    const FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    const auto category = FunctionDocumentation::Category::Other;
    const FunctionDocumentation::Arguments date_or_timestamp
        = {{"value", "The value to transform.", {"Date", "Date32", "DateTime", "DateTime64"}}};
    const FunctionDocumentation::Arguments timestamp = {{"value", "The value to transform.", {"DateTime", "DateTime64"}}};

    factory.registerFunction<FunctionIcebergDateTransform<IcebergDateTransform::Year>>(FunctionDocumentation{
        .description = R"(Implements the Iceberg `year` partition transform: the number of years from 1970, computed in UTC.
See https://iceberg.apache.org/spec/#partition-transforms.)",
        .syntax = "icebergYear(value)",
        .arguments = date_or_timestamp,
        .returned_value = {"Years from 1970, negative for values before 1970", {"Int32"}},
        .examples = {{"Example", "SELECT icebergYear(toDate32('1969-05-05'))", "-1"}},
        .introduced_in = introduced_in,
        .category = category});

    factory.registerFunction<FunctionIcebergDateTransform<IcebergDateTransform::Month>>(FunctionDocumentation{
        .description = R"(Implements the Iceberg `month` partition transform: the number of months from 1970-01-01, computed in UTC.
See https://iceberg.apache.org/spec/#partition-transforms.)",
        .syntax = "icebergMonth(value)",
        .arguments = date_or_timestamp,
        .returned_value = {"Months from 1970-01-01, negative for values before 1970", {"Int32"}},
        .examples = {{"Example", "SELECT icebergMonth(toDate32('1969-01-01'))", "-12"}},
        .introduced_in = introduced_in,
        .category = category});

    factory.registerFunction<FunctionIcebergDateTransform<IcebergDateTransform::Day>>(FunctionDocumentation{
        .description = R"(Implements the Iceberg `day` partition transform: the number of days from 1970-01-01, computed in UTC.
See https://iceberg.apache.org/spec/#partition-transforms.)",
        .syntax = "icebergDay(value)",
        .arguments = date_or_timestamp,
        .returned_value = {"Days from 1970-01-01, negative for values before 1970", {"Int32"}},
        .examples = {{"Example", "SELECT icebergDay(toDate32('1969-12-31'))", "-1"}},
        .introduced_in = introduced_in,
        .category = category});

    factory.registerFunction<FunctionIcebergDateTransform<IcebergDateTransform::Hour>>(FunctionDocumentation{
        .description
        = R"(Implements the Iceberg `hour` partition transform: the number of hours from 1970-01-01 00:00:00, computed in UTC.
See https://iceberg.apache.org/spec/#partition-transforms.)",
        .syntax = "icebergHour(value)",
        .arguments = timestamp,
        .returned_value = {"Hours from 1970-01-01 00:00:00, negative for values before 1970", {"Int32"}},
        .examples = {{"Example", "SELECT icebergHour(toDateTime64('1970-01-01 01:30:00', 6, 'UTC'))", "1"}},
        .introduced_in = introduced_in,
        .category = category});
}

}
