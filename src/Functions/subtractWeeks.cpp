#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;

REGISTER_FUNCTION(SubtractWeeks)
{
    FunctionDocumentation::Description description_subtractWeeks = R"(
Subtracts a specified number of weeks from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractWeeks = R"(
subtractWeeks(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractWeeks = {
        {"datetime", "Date or date with time to subtract specified number of weeks from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of weeks to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractWeeks = "Returns `datetime` minus `num` weeks. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractWeeks = {
        {"Subtract weeks from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractWeeks(date, 1) AS subtract_weeks_with_date,
    subtractWeeks(date_time, 1) AS subtract_weeks_with_date_time,
    subtractWeeks(date_time_string, 1) AS subtract_weeks_with_date_time_string
        )",
        R"(
┌─subtract_weeks_with_date─┬─subtract_weeks_with_date_time─┬─subtract_weeks_with_date_time_string─┐
│               2023-12-25 │           2023-12-25 00:00:00 │              2023-12-25 00:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 week)
        )",
        R"(
┌─minus(CAST('⋯alWeek(10))─┐
│               1998-04-07 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractWeeks = {1, 1};
    FunctionDocumentation::Category category_subtractWeeks = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractWeeks = {
        description_subtractWeeks,
        syntax_subtractWeeks,
        arguments_subtractWeeks,
        returned_value_subtractWeeks,
        examples_subtractWeeks,
        introduced_in_subtractWeeks,
        category_subtractWeeks
    };

    factory.registerFunction<FunctionSubtractWeeks>(documentation_subtractWeeks);
}

}


