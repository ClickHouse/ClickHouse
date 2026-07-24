#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeYearNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of years elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used
standalone. The main purpose of the function is to calculate the difference in years between two dates or dates with time, e.g., `toRelativeYearNum(dt1) - toRelativeYearNum(dt2)`.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeYearNum(date)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of years from a fixed reference point in the past.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
        {"Get relative year numbers", R"(
SELECT toRelativeYearNum('2010-10-01'::DateTime) - toRelativeYearNum('2000-01-01'::DateTime)
        )",
        R"(
┌─minus(toRela⋯ateTime')))─┐
│                       10 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeYearNum>(documentation);
}

}


