#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddDays = FunctionDateOrDateTimeAddInterval<AddDaysImpl>;

REGISTER_FUNCTION(AddDays)
{
    FunctionDocumentation::Description description_addDays = R"(
Adds a specified number of days to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addDays = R"(
addDays(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addDays = {
        {"datetime", "Date or date with time to add specified number of days to. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of days to add. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_addDays = "Returns `datetime` plus `num` days. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_addDays = {
        {"Add days to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addDays(date, 5) AS add_days_with_date,
    addDays(date_time, 5) AS add_days_with_date_time,
    addDays(date_time_string, 5) AS add_days_with_date_time_string
        )",
        R"(
┌─add_days_with_date─┬─add_days_with_date_time─┬─add_days_with_date_time_string─┐
│         2024-01-06 │     2024-01-06 00:00:00 │        2024-01-06 00:00:00.000 │
└────────────────────┴─────────────────────────┴────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 day)
        )",
        R"(
┌─plus(CAST('1⋯valDay(10))─┐
│               1998-06-26 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addDays = {1, 1};
    FunctionDocumentation::Category category_addDays = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addDays = {
        description_addDays,
        syntax_addDays,
        arguments_addDays,
        returned_value_addDays,
        examples_addDays,
        introduced_in_addDays,
        category_addDays
    };

    factory.registerFunction<FunctionAddDays>(documentation_addDays);
}

}


