#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMinute = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMinuteImpl>;

REGISTER_FUNCTION(ToMinute)
{

    FunctionDocumentation::Description description_to_minute = R"(
Returns the minute component (0-59) of a `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax_to_minute = "toMinute(datetime)";
    FunctionDocumentation::Arguments arguments_to_minute =
    {
        {"datetime", "Date or date with time to get the minute from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_minute = {"Returns the minute of the hour (0 - 59) of the given `Date` or `DateTime` value", {"UInt8"}};
    FunctionDocumentation::Examples examples_to_minute = {
        {"Usage example", R"(
SELECT toMinute(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toMinute(toDateTime('2023-04-21 10:20:30'))─┐
│                                          20 │
└─────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_minute = {1, 1};
    FunctionDocumentation::Category category_to_minute = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_minute = {description_to_minute, syntax_to_minute, arguments_to_minute, returned_value_to_minute, examples_to_minute, introduced_in_to_minute, category_to_minute};

    factory.registerFunction<FunctionToMinute>(documentation_to_minute);

    /// MySQL compatibility alias.
    factory.registerAlias("MINUTE", "toMinute", FunctionFactory::Case::Insensitive);
}

}
