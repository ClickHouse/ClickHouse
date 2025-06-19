#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToHourImpl>;

REGISTER_FUNCTION(ToHour)
{
    FunctionDocumentation::Description description = R"(
Returns the hour component (0-23) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax = "toHour(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A `Date` or `DateTime` value to get the hour from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The hour of the given `Date` or `DateTime` value. [`UInt8`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
    SELECT toHour(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
    ┌─toHour(toDateTime('2023-04-21 10:20:30'))─┐
    │                                        10 │
    └───────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToHour>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("HOUR", "toHour", FunctionFactory::Case::Insensitive);
}

}
