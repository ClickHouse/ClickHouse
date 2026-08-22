#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;

REGISTER_FUNCTION(ToDayOfYear)
{
    FunctionDocumentation::Description description = R"(
Returns the number of the day within the year (1-366) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax = "toDayOfYear(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the day of year from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the day of the year of the given Date or DateTime", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toDayOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                            111 │
└────────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDayOfYear>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFYEAR", "toDayOfYear", FunctionFactory::Case::Insensitive);
}

}
