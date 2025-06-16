#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMinutes = FunctionDateOrDateTimeAddInterval<SubtractMinutesImpl>;

REGISTER_FUNCTION(SubtractMinutes)
{
    FunctionDocumentation::Description description_subtractMinutes = R"(
Subtracts a specified number of minutes from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractMinutes = R"(
subtractMinutes(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractMinutes = {
        {"datetime", "Date or date with time to subtract specified number of minutes from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of minutes to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractMinutes = "Returns `datetime` minus `num` minutes. [`DateTime`](../data-types/datetime.md)/[`DateTime64(3)`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractMinutes = {
        {"Subtract minutes from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMinutes(date, 30) AS subtract_minutes_with_date,
    subtractMinutes(date_time, 30) AS subtract_minutes_with_date_time,
    subtractMinutes(date_time_string, 30) AS subtract_minutes_with_date_time_string
        )",
        R"(
┌─subtract_minutes_with_date─┬─subtract_minutes_with_date_time─┬─subtract_minutes_with_date_time_string─┐
│        2023-12-31 23:30:00 │             2023-12-31 23:30:00 │                2023-12-31 23:30:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 minute)
        )",
        R"(
┌─minus(CAST('⋯Minute(10))─┐
│      1998-06-15 23:50:00 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractMinutes = {1, 1};
    FunctionDocumentation::Category category_subtractMinutes = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractMinutes = {
        description_subtractMinutes,
        syntax_subtractMinutes,
        arguments_subtractMinutes,
        returned_value_subtractMinutes,
        examples_subtractMinutes,
        introduced_in_subtractMinutes,
        category_subtractMinutes
    };

    factory.registerFunction<FunctionSubtractMinutes>(documentation_subtractMinutes);
}

}


