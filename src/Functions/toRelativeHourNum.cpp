#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl<ResultPrecision::Standard>>;

REGISTER_FUNCTION(ToRelativeHourNum)
{
    FunctionDocumentation::Description description_toRelativeHourNum = R"(
Converts a date or date with time to the number of hours elapsed since a certain fixed point in the past.
    )";
    FunctionDocumentation::Syntax syntax_toRelativeHourNum = R"(
toRelativeHourNum(date)
    )";
    FunctionDocumentation::Arguments arguments_toRelativeHourNum = {
        {"date", "Date or date with time. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toRelativeHourNum = "Returns the number of hours from a fixed reference point in the past. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toRelativeHourNum = {
        {"Get relative hour numbers", R"(
SELECT
toRelativeHourNum(toDateTime('1993-10-05 05:20:36')) AS h1,
toRelativeHourNum(toDateTime('2000-09-20 14:11:29')) AS h2
        )",
        R"(
┌─────h1─┬─────h2─┐
│ 208276 │ 269292 │
└────────┴────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toRelativeHourNum = {1, 1};
    FunctionDocumentation::Category category_toRelativeHourNum = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toRelativeHourNum = {
        description_toRelativeHourNum,
        syntax_toRelativeHourNum,
        arguments_toRelativeHourNum,
        returned_value_toRelativeHourNum,
        examples_toRelativeHourNum,
        introduced_in_toRelativeHourNum,
        category_toRelativeHourNum
    };

    factory.registerFunction<FunctionToRelativeHourNum>(documentation_toRelativeHourNum);
}

}


