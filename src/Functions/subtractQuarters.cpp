#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractQuarters = FunctionDateOrDateTimeAddInterval<SubtractQuartersImpl>;

REGISTER_FUNCTION(SubtractQuarters)
{
    FunctionDocumentation::Description description_subtractQuarters = R"(
Subtracts a specified number of quarters from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractQuarters = R"(
subtractQuarters(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractQuarters = {
        {"datetime", "Date or date with time to subtract specified number of quarters from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of quarters to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractQuarters = "Returns `datetime` minus `num` quarters. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractQuarters = {
        {"Subtract quarters from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractQuarters(date, 1) AS subtract_quarters_with_date,
    subtractQuarters(date_time, 1) AS subtract_quarters_with_date_time,
    subtractQuarters(date_time_string, 1) AS subtract_quarters_with_date_time_string
        )",
        R"(
┌─subtract_quarters_with_date─┬─subtract_quarters_with_date_time─┬─subtract_quarters_with_date_time_string─┐
│                  2023-10-01 │              2023-10-01 00:00:00 │                 2023-10-01 00:00:00.000 │
└─────────────────────────────┴──────────────────────────────────┴─────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 quarter)
        )",
        R"(
┌─minus(CAST('1⋯Quarter(10))─┐
│                1996-09-16 │
└───────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractQuarters = {20, 1};
    FunctionDocumentation::Category category_subtractQuarters = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractQuarters = {
        description_subtractQuarters,
        syntax_subtractQuarters,
        arguments_subtractQuarters,
        returned_value_subtractQuarters,
        examples_subtractQuarters,
        introduced_in_subtractQuarters,
        category_subtractQuarters
    };

    factory.registerFunction<FunctionSubtractQuarters>(documentation_subtractQuarters);
}

}


