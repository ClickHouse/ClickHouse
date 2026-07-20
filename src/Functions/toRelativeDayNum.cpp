#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeDayNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of days elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in days between two dates or dates with time, e.g., `toRelativeDayNum(dt1) - toRelativeDayNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeDayNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of days from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative day numbers", R"(
SELECT toRelativeDayNum(toDate('2023-04-01')) - toRelativeDayNum(toDate('2023-01-01'))
        )",
        R"(
┌─minus(toRela⋯3-01-01')))─┐
│                       90 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeDayNum>(documentation);
}

}


