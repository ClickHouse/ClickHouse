#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractSeconds = FunctionDateOrDateTimeAddInterval<SubtractSecondsImpl>;

REGISTER_FUNCTION(SubtractSeconds)
{
    FunctionDocumentation::Description description_subtractSeconds = R"(
Subtracts a specified number of seconds from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractSeconds = R"(
subtractSeconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractSeconds = {
        {"datetime", "Date or date with time to subtract specified number of seconds from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of seconds to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractSeconds = "Returns `datetime` minus `num` seconds. [`DateTime`](../data-types/datetime.md)/[`DateTime64(3)`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractSeconds = {
        {"Subtract seconds from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractSeconds(date, 60) AS subtract_seconds_with_date,
    subtractSeconds(date_time, 60) AS subtract_seconds_with_date_time,
    subtractSeconds(date_time_string, 60) AS subtract_seconds_with_date_time_string
        )",
        R"(
┌─subtract_seconds_with_date─┬─subtract_seconds_with_date_time─┬─subtract_seconds_with_date_time_string─┐
│        2023-12-31 23:59:00 │             2023-12-31 23:59:00 │                2023-12-31 23:59:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 second)
        )",
        R"(
┌─minus(CAST('⋯Second(10))─┐
│      1998-06-15 23:59:50 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractSeconds = {1, 1};
    FunctionDocumentation::Category category_subtractSeconds = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractSeconds = {
        description_subtractSeconds,
        syntax_subtractSeconds,
        arguments_subtractSeconds,
        returned_value_subtractSeconds,
        examples_subtractSeconds,
        introduced_in_subtractSeconds,
        category_subtractSeconds
    };

    factory.registerFunction<FunctionSubtractSeconds>(documentation_subtractSeconds);
}

}


