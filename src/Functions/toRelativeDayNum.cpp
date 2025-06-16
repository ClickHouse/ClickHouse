#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeDayNum)
{
    FunctionDocumentation::Description description_toRelativeDayNum = R"(
Converts a date or date with time to the number of days elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeDayNum = R"(
toRelativeDayNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeDayNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeDayNum = "Returns the number of days from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeDayNum =
    {
        {"Get relative day numbers", R"(
SELECT
  toRelativeDayNum(toDate('1993-10-05')) AS d1,
  toRelativeDayNum(toDate('2000-09-20')) AS d2
        )",
        R"(
┌───d1─┬────d2─┐
│ 8678 │ 11220 │
└──────┴───────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeDayNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeDayNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeDayNum =
    {
        description_toRelativeDayNum,
        syntax_toRelativeDayNum,
        arguments_toRelativeDayNum,
        returned_value_toRelativeDayNum,
        examples_toRelativeDayNum,
        introduced_in_toRelativeDayNum,
        category_toRelativeDayNum
    };

    factory.registerFunction<FunctionToRelativeDayNum>(documentation_toRelativeDayNum);
}

}


