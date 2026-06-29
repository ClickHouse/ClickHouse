#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDaysInMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDaysInMonthImpl>;

REGISTER_FUNCTION(ToDaysInMonth)
{
    FunctionDocumentation::Description description = R"(
Returns the number of days in the month of a `Date` or `DateTime`.

The returned value is in the range 28 to 31.
    )";
    FunctionDocumentation::Syntax syntax = "toDaysInMonth(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the number of days in the month from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of days in the month of the given date/time.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDaysInMonth(toDate('2023-02-01')), toDaysInMonth(toDate('2024-02-01')), toDaysInMonth(toDate('2023-01-01'))
        )",
        R"(
┌─toDaysInMonth(toDate('2023-02-01'))─┬─toDaysInMonth(toDate('2024-02-01'))─┬─toDaysInMonth(toDate('2023-01-01'))─┐
│                                  28 │                                  29 │                                  31 │
└─────────────────────────────────────┴─────────────────────────────────────┴─────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDaysInMonth>(documentation);
}

}
