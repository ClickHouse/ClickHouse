#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;

REGISTER_FUNCTION(ToDayOfMonth)
{
    FunctionDocumentation::Description description = R"(
Returns the day of the month (1-31) of a `Date` or `DateTime`.
        )";
    FunctionDocumentation::Syntax syntax = "toDayOfMonth(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the day of month from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the day of the month of the given date/time", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDayOfMonth(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                              21 │
└─────────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDayOfMonth>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("DAY", "toDayOfMonth", FunctionFactory::Case::Insensitive);
    factory.registerAlias("DAYOFMONTH", "toDayOfMonth", FunctionFactory::Case::Insensitive);
}

}
