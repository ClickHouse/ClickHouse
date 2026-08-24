#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeWeekNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of weeks elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in weeks between two dates or dates with time, e.g., `toRelativeWeekNum(dt1) - toRelativeWeekNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeWeekNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of weeks from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative week numbers", R"(
SELECT toRelativeWeekNum(toDate('2023-01-08')) - toRelativeWeekNum(toDate('2023-01-01')) AS weeks_difference
        )",
        R"(
┌─weeks_difference─┐
│                1 │
└──────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeWeekNum>(documentation);
}

}


