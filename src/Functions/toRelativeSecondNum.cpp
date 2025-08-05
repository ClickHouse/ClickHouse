#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeSecondNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of seconds elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in seconds between two dates or dates with time, e.g., `toRelativeSecondNum(dt1) - toRelativeSecondNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeSecondNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of seconds from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative second numbers", R"(
SELECT toRelativeSecondNum(toDateTime('2023-01-01 00:01:00')) - toRelativeSecondNum(toDateTime('2023-01-01 00:00:00')) AS seconds_difference
        )",
        R"(
┌─seconds_difference─┐
│                 60 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeSecondNum>(documentation);
}

}


