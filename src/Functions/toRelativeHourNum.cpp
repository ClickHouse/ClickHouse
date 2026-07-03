#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeHourNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of hours elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in hours between two dates or dates with time, e.g., `toRelativeHourNum(dt1) - toRelativeHourNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeHourNum(date)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of hours from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Get relative hour numbers", R"(
SELECT toRelativeHourNum(toDateTime('2023-01-01 12:00:00')) - toRelativeHourNum(toDateTime('2023-01-01 00:00:00')) AS hours_difference
        )",
        R"(
┌─hours_difference─┐
│               12 │
└──────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeHourNum>(documentation);
}

}


