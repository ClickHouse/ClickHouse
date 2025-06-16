#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeQuarterNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeQuarterNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeQuarterNum)
{
    FunctionDocumentation::Description description_toRelativeQuarterNum = R"(
Converts a date or date with time to the number of quarters elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeQuarterNum = R"(
toRelativeQuarterNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeQuarterNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeQuarterNum = "Returns the number of quarters from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeQuarterNum =
    {
        {"Get relative quarter numbers", R"(
SELECT
  toRelativeQuarterNum(toDate('1993-11-25')) AS q1,
  toRelativeQuarterNum(toDate('2005-01-05')) AS q2
        )",
        R"(
┌───q1─┬───q2─┐
│ 7975 │ 8020 │
└──────┴──────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeQuarterNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeQuarterNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeQuarterNum =
    {
        description_toRelativeQuarterNum,
        syntax_toRelativeQuarterNum,
        arguments_toRelativeQuarterNum,
        returned_value_toRelativeQuarterNum,
        examples_toRelativeQuarterNum,
        introduced_in_toRelativeQuarterNum,
        category_toRelativeQuarterNum
    };

    factory.registerFunction<FunctionToRelativeQuarterNum>(documentation_toRelativeQuarterNum);
}

}


