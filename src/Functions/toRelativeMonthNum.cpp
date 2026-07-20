#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMonthNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeMonthNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of months elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in months between two dates or dates with time, e.g., `toRelativeMonthNum(dt1) - toRelativeMonthNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeMonthNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of months from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative month numbers", R"(
SELECT toRelativeMonthNum(toDate('2023-04-01')) - toRelativeMonthNum(toDate('2023-01-01')) AS months_difference
        )",
        R"(
┌─months_difference─┐
│                 3 │
└───────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeMonthNum>(documentation);
}

}


