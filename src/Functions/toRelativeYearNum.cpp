#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeYearNum = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToRelativeYearNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeYearNum)
{
    FunctionDocumentation::Description description_toRelativeYearNum = R"(
Converts a date or date with time to the number of years elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeYearNum = R"(
toRelativeYearNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeYearNum = {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeYearNum = "Returns the number of years from a fixed reference point in the past. [`UInt16`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeYearNum = {
        {"Get relative year numbers", R"(
SELECT
    toRelativeYearNum(toDate('2002-12-08')) AS y1,
    toRelativeYearNum(toDate('2010-10-26')) AS y2
        )",
        R"(
┌───y1─┬───y2─┐
│ 2002 │ 2010 │
└──────┴──────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeYearNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeYearNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeYearNum = {
        description_toRelativeYearNum,
        syntax_toRelativeYearNum,
        arguments_toRelativeYearNum,
        returned_value_toRelativeYearNum,
        examples_toRelativeYearNum,
        introduced_in_toRelativeYearNum,
        category_toRelativeYearNum
    };

    factory.registerFunction<FunctionToRelativeYearNum>(documentation_toRelativeYearNum);
}

}


