#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMonthNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMonthNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeMonthNum)
{
    FunctionDocumentation::Description description_toRelativeMonthNum = R"(
Converts a date or date with time to the number of months elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeMonthNum = R"(
toRelativeMonthNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeMonthNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeMonthNum = "Returns the number of months from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeMonthNum =
    {
        {"Get relative month numbers", R"(
SELECT
  toRelativeMonthNum(toDate('2001-04-25')) AS m1,
  toRelativeMonthNum(toDate('2009-07-08')) AS m2
        )",
        R"(
┌────m1─┬────m2─┐
│ 24016 │ 24115 │
└───────┴───────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeMonthNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeMonthNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeMonthNum =
    {
        description_toRelativeMonthNum,
        syntax_toRelativeMonthNum,
        arguments_toRelativeMonthNum,
        returned_value_toRelativeMonthNum,
        examples_toRelativeMonthNum,
        introduced_in_toRelativeMonthNum,
        category_toRelativeMonthNum
    };

    factory.registerFunction<FunctionToRelativeMonthNum>(documentation_toRelativeMonthNum);
}

}


