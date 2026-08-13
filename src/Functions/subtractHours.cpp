#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractHours = FunctionDateOrDateTimeAddInterval<SubtractHoursImpl>;

REGISTER_FUNCTION(SubtractHours)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of hours from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractHours(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to subtract specified number of hours from.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of hours to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` hours", {"DateTime", "DateTime64(3)"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractHours>(documentation);
}

}


