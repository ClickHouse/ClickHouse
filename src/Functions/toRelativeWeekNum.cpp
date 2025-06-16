#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeWeekNum)
{
    FunctionDocumentation::Description description_toRelativeWeekNum = R"(
Converts a date or date with time to the number of weeks elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeWeekNum = R"(
toRelativeWeekNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeWeekNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeWeekNum = "Returns the number of weeks from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeWeekNum =
    {
        {"Get relative week numbers", R"(
SELECT
toRelativeWeekNum(toDate('2000-02-29')) AS w1,
toRelativeWeekNum(toDate('2001-01-12')) AS w2
        )",
        R"(
┌───w1─┬───w2─┐
│ 1574 │ 1619 │
└──────┴──────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeWeekNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeWeekNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeWeekNum =
    {
        description_toRelativeWeekNum,
        syntax_toRelativeWeekNum,
        arguments_toRelativeWeekNum,
        returned_value_toRelativeWeekNum,
        examples_toRelativeWeekNum,
        introduced_in_toRelativeWeekNum,
        category_toRelativeWeekNum
    };

    factory.registerFunction<FunctionToRelativeWeekNum>(documentation_toRelativeWeekNum);
}

}


