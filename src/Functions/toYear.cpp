#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;

REGISTER_FUNCTION(ToYear)
{
    FunctionDocumentation::Description description = R"(
Returns the year component (AD) of a `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax = "toYear(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the year from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the year of the given Date or DateTime", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT toYear(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                     2023  │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToYear>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("YEAR", "toYear", FunctionFactory::Case::Insensitive);
}

}
