#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMonths = FunctionDateOrDateTimeAddInterval<AddMonthsImpl>;

REGISTER_FUNCTION(AddMonths)
{
    FunctionDocumentation::Description description_addMonths = R"(
Adds a specified number of months to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addMonths = R"(
addMonths(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addMonths = {
        {"datetime", "Date or date with time to add specified number of months to. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of months to add. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_addMonths = "Returns `datetime` plus `num` months. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_addMonths = {
        {"Add months to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMonths(date, 6) AS add_months_with_date,
    addMonths(date_time, 6) AS add_months_with_date_time,
    addMonths(date_time_string, 6) AS add_months_with_date_time_string
        )",
        R"(
┌─add_months_with_date─┬─add_months_with_date_time─┬─add_months_with_date_time_string─┐
│           2024-07-01 │       2024-07-01 00:00:00 │          2024-07-01 00:00:00.000 │
└──────────────────────┴───────────────────────────┴──────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 month)
        )",
        R"(
┌─plus(CAST('1⋯lMonth(10))─┐
│               1999-04-16 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addMonths = {1, 1};
    FunctionDocumentation::Category category_addMonths = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addMonths = {
        description_addMonths,
        syntax_addMonths,
        arguments_addMonths,
        returned_value_addMonths,
        examples_addMonths,
        introduced_in_addMonths,
        category_addMonths
    };

    factory.registerFunction<FunctionAddMonths>(documentation_addMonths);
}

}


