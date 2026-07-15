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
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the hour from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The hour of the given `Date` or `DateTime` value", {"UInt8"}};
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
