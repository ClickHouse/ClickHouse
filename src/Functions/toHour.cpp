#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToHour = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToHourImpl>;

REGISTER_FUNCTION(ToHour)
{
    FunctionDocumentation::Description description_to_hour = R"(
Returns the hour component (0-23) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax_to_hour = "toHour(datetime)";
    FunctionDocumentation::Arguments arguments_to_hour =
    {
        {"datetime", "Date or date with time to get the hour from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_hour = {"The hour of the given `Date` or `DateTime` value", {"UInt8"}};
    FunctionDocumentation::Examples examples_to_hour = {
        {"Usage example", R"(
    SELECT toHour(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
    ┌─toHour(toDateTime('2023-04-21 10:20:30'))─┐
    │                                        10 │
    └───────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_hour = {1, 1};
    FunctionDocumentation::Category category_to_hour = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_hour = {description_to_hour, syntax_to_hour, arguments_to_hour, returned_value_to_hour, examples_to_hour, introduced_in_to_hour, category_to_hour};

    factory.registerFunction<FunctionToHour>(documentation_to_hour);

    /// MySQL compatibility alias.
    factory.registerAlias("HOUR", "toHour", FunctionFactory::Case::Insensitive);
}

}
