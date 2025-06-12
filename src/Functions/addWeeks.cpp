#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddWeeks = FunctionDateOrDateTimeAddInterval<AddWeeksImpl>;

REGISTER_FUNCTION(AddWeeks)
{
    FunctionDocumentation::Description description_addWeeks = R"(
Adds a specified number of weeks to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addWeeks = R"(
addWeeks(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addWeeks = {
        {"datetime", "Date or date with time to add specified number of weeks to. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of weeks to add. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_addWeeks = "Returns `datetime` plus `num` weeks. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_addWeeks = {
        {"Add weeks to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addWeeks(date, 5) AS add_weeks_with_date,
    addWeeks(date_time, 5) AS add_weeks_with_date_time,
    addWeeks(date_time_string, 5) AS add_weeks_with_date_time_string
        )",
        R"(
┌─add_weeks_with_date─┬─add_weeks_with_date_time─┬─add_weeks_with_date_time_string─┐
│          2024-02-05 │      2024-02-05 00:00:00 │         2024-02-05 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 week)
        )",
        R"(
┌─plus(CAST('1⋯alWeek(10))─┐
│               1998-08-25 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addWeeks = {1, 1};
    FunctionDocumentation::Category category_addWeeks = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addWeeks = {
        description_addWeeks,
        syntax_addWeeks,
        arguments_addWeeks,
        returned_value_addWeeks,
        examples_addWeeks,
        introduced_in_addWeeks,
        category_addWeeks
    };

    factory.registerFunction<FunctionAddWeeks>(documentation_addWeeks);
}

}


