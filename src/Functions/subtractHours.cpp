#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;

REGISTER_FUNCTION(SubtractHours)
{
    FunctionDocumentation::Description description_subtractHours = R"(
Subtracts a specified number of hours from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractHours = R"(
subtractHours(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractHours = {
        {"datetime", "Date or date with time to subtract specified number of hours from. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of hours to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractHours = "Returns `datetime` minus `num` hours. [`DateTime`](../data-types/datetime.md)/[`DateTime64(3)`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractHours = {
        {"Subtract hours from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractHours(date, 12) AS subtract_hours_with_date,
    subtractHours(date_time, 12) AS subtract_hours_with_date_time,
    subtractHours(date_time_string, 12) AS subtract_hours_with_date_time_string
        )",
        R"(
┌─subtract_hours_with_date─┬─subtract_hours_with_date_time─┬─subtract_hours_with_date_time_string─┐
│      2023-12-31 12:00:00 │           2023-12-31 12:00:00 │              2023-12-31 12:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 hour)
        )",
        R"(
┌─minus(CAST('⋯alHour(10))─┐
│      1998-06-15 14:00:00 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractHours = {1, 1};
    FunctionDocumentation::Category category_subtractHours = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractHours = {
        description_subtractHours,
        syntax_subtractHours,
        arguments_subtractHours,
        returned_value_subtractHours,
        examples_subtractHours,
        introduced_in_subtractHours,
        category_subtractHours
    };

    factory.registerFunction<FunctionSubtractHours>(documentation_subtractHours);
}

}


