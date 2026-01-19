#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeMinuteNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of minutes elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in minutes between two dates or dates with time, e.g., `toRelativeMinuteNum(dt1) - toRelativeMinuteNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeMinuteNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of minutes from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative minute numbers", R"(
SELECT toRelativeMinuteNum(toDateTime('2023-01-01 00:30:00')) - toRelativeMinuteNum(toDateTime('2023-01-01 00:00:00')) AS minutes_difference
        )",
        R"(
┌─minutes_difference─┐
│                 30 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeMinuteNum>(documentation);
}

}


