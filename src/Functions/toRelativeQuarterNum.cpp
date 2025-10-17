#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeQuarterNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeQuarterNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeQuarterNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of quarters elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in quarters between two dates or dates with time, e.g., `toRelativeQuarterNum(dt1) - toRelativeQuarterNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeQuarterNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of quarters from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative quarter numbers", R"(
SELECT toRelativeQuarterNum(toDate('2023-04-01')) - toRelativeQuarterNum(toDate('2023-01-01')) AS quarters_difference
        )",
        R"(
┌─quarters_difference─┐
│                   1 │
└─────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeQuarterNum>(documentation);
}

}


