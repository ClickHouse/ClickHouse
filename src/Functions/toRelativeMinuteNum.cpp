#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeMinuteNum)
{
    FunctionDocumentation::Description description_toRelativeMinuteNum = R"(
Converts a date or date with time to the number of minutes elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeMinuteNum = R"(
toRelativeMinuteNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeMinuteNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeMinuteNum = "Returns the number of minutes from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeMinuteNum =
    {
        {"Get relative minute numbers", R"(
SELECT
toRelativeMinuteNum(toDateTime('1993-10-05 05:20:36')) AS m1,
toRelativeMinuteNum(toDateTime('2000-09-20 14:11:29')) AS m2
        )",
        R"(
┌───────m1─┬───────m2─┐
│ 12496580 │ 16157531 │
└──────────┴──────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeMinuteNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeMinuteNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeMinuteNum =
    {
        description_toRelativeMinuteNum,
        syntax_toRelativeMinuteNum,
        arguments_toRelativeMinuteNum,
        returned_value_toRelativeMinuteNum,
        examples_toRelativeMinuteNum,
        introduced_in_toRelativeMinuteNum,
        category_toRelativeMinuteNum
    };

    factory.registerFunction<FunctionToRelativeMinuteNum>(documentation_toRelativeMinuteNum);
}

}


