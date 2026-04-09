#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeInterval.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionToInterval : public IFunction
{
public:
    static constexpr auto name = "toInterval";

    explicit FunctionToInterval(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionToInterval>(context); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        return { .is_monotonic = true, .is_always_monotonic = true };
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} must be 2 arguments", getName());

        /// The second argument is a constant string with the name of interval kind.
        String interval_kind;
        const ColumnConst * kind_column = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!kind_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be constant string: "
                "unit of interval", getName());

        interval_kind = Poco::toLower(kind_column->getValue<String>());
        if (interval_kind.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second argument (unit) for function {} cannot be empty", getName());

        if (!IntervalKind::tryParseString(interval_kind, kind.kind))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} doesn't look like an interval unit in {}", interval_kind, getName());

        return std::make_shared<DataTypeInterval>(kind);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        ColumnsWithTypeAndName temp_columns(1);
        temp_columns[0] = arguments[0];

        const char * to_interval_function_name = kind.toNameOfFunctionToIntervalDataType();
        auto to_interval_function = FunctionFactory::instance().get(to_interval_function_name, context);

        return to_interval_function->build(temp_columns)->execute(temp_columns, result_type, input_rows_count, /* dry run = */ false);
    }

private:
    ContextPtr context;
    mutable IntervalKind kind = IntervalKind::Kind::Second;
};

REGISTER_FUNCTION(ToInterval)
{
    FunctionDocumentation::Description description = R"(
Creates an Interval value from a numeric value and a unit string.

This function provides a unified way to create intervals of different types (seconds, minutes, hours, days, weeks, months, quarters, years)
from a single function by specifying the unit as a string argument. The unit string is case-insensitive.

This is equivalent to calling type-specific functions like `toIntervalSecond`, `toIntervalMinute`, `toIntervalDay`, etc.,
but allows the unit to be specified dynamically as a string parameter.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toInterval(value, unit)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The numeric value representing the number of units. Can be any numeric type.", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64"}},
        {"unit", R"(The unit of time. Must be a constant string. Valid values: 'nanosecond', 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year'.)", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {R"(Returns an Interval value of the specified type. The result type depends on the unit: IntervalNanosecond, IntervalMicrosecond, IntervalMillisecond, IntervalSecond, IntervalMinute, IntervalHour, IntervalDay, IntervalWeek, IntervalMonth, IntervalQuarter, or IntervalYear.)", {"Interval"}};
    FunctionDocumentation::Examples examples = {
        {"Create intervals with different units", R"(
SELECT
    toInterval(5, 'second') AS seconds,
    toInterval(3, 'day') AS days,
    toInterval(2, 'month') AS months
        )",
        R"(
┌─seconds─┬─days─┬─months─┐
│ 5       │ 3    │ 2      │
└─────────┴──────┴────────┘
        )"},
        {"Use intervals in date arithmetic", R"(
SELECT
    now() AS current_time,
    now() + toInterval(1, 'hour') AS one_hour_later,
    now() - toInterval(7, 'day') AS week_ago
        )",
        R"(
┌─────────current_time─┬──one_hour_later─────┬────────────week_ago─┐
│ 2025-01-04 10:30:00  │ 2025-01-04 11:30:00 │ 2024-12-28 10:30:00 │
└──────────────────────┴─────────────────────┴─────────────────────┘
        )"},
        {"Dynamic interval creation", R"(
SELECT toDate('2025-01-01') + toInterval(number, 'day') AS dates
FROM numbers(5)
        )",
        R"(
┌──────dates─┐
│ 2025-01-01 │
│ 2025-01-02 │
│ 2025-01-03 │
│ 2025-01-04 │
│ 2025-01-05 │
└────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToInterval>(documentation);
}

}
