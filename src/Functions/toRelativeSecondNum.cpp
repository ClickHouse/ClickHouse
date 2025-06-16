#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeSecondNum)
{
    FunctionDocumentation::Description description_toRelativeSecondNum = R"(
Converts a date or date with time to the number of the seconds elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeSecondNum = R"(
toRelativeSecondNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeSecondNum =
    {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeSecondNum = "Returns the number of seconds from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeSecondNum =
    {
        {"Get relative second numbers", R"(
SELECT
toRelativeSecondNum(toDateTime('1993-10-05 05:20:36')) AS s1,
toRelativeSecondNum(toDateTime('2000-09-20 14:11:29')) AS s2
        )",
        R"(
┌────────s1─┬────────s2─┐
│ 749794836 │ 969451889 │
└───────────┴───────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeSecondNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeSecondNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeSecondNum =
    {
        description_toRelativeSecondNum,
        syntax_toRelativeSecondNum,
        arguments_toRelativeSecondNum,
        returned_value_toRelativeSecondNum,
        examples_toRelativeSecondNum,
        introduced_in_toRelativeSecondNum,
        category_toRelativeSecondNum
    };

    factory.registerFunction<FunctionToRelativeSecondNum>(documentation_toRelativeSecondNum);
}

}


