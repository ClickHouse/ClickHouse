#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToSecondImpl>;

REGISTER_FUNCTION(ToSecond)
{
    FunctionDocumentation::Description description = R"(
Returns the second component (0-59) of a `DateTime` or `DateTime64` value.
        )";
    FunctionDocumentation::Syntax syntax = "toSecond(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to get the second from.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the second in the minute (0 - 59) of `datetime`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toSecond(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toSecond(toDateTime('2023-04-21 10:20:30'))─┐
│                                          30 │
└─────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToSecond>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("SECOND", "toSecond", FunctionFactory::Case::Insensitive);
}

}
