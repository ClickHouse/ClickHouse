#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;

REGISTER_FUNCTION(SubtractDays)
{
    FunctionDocumentation::Description description_subtractDays = R"(
Subtracts a specified number of days from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractDays = R"(
subtractDays(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractDays = {
        {"datetime", "Date or date with time to subtract specified number of days from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of days to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractDays = "Returns `datetime` minus `num` days. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractDays = {
        {"Subtract days from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractDays(date, 31) AS subtract_days_with_date,
    subtractDays(date_time, 31) AS subtract_days_with_date_time,
    subtractDays(date_time_string, 31) AS subtract_days_with_date_time_string
        )",
        R"(
┌─subtract_days_with_date─┬─subtract_days_with_date_time─┬─subtract_days_with_date_time_string─┐
│              2023-12-01 │          2023-12-01 00:00:00 │             2023-12-01 00:00:00.000 │
└─────────────────────────┴──────────────────────────────┴─────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 day)
        )",
        R"(
┌─minus(CAST('⋯valDay(10))─┐
│               1998-06-06 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractDays = {1, 1};
    FunctionDocumentation::Category category_subtractDays = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractDays = {
        description_subtractDays,
        syntax_subtractDays,
        arguments_subtractDays,
        returned_value_subtractDays,
        examples_subtractDays,
        introduced_in_subtractDays,
        category_subtractDays
    };

    factory.registerFunction<FunctionSubtractDays>(documentation_subtractDays);
}

}


