#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractWeeks = FunctionDateOrDateTimeAddInterval<SubtractWeeksImpl>;

REGISTER_FUNCTION(SubtractWeeks)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of weeks from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractWeeks(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to subtract specified number of weeks from.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of weeks to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` weeks", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractWeeks>(documentation);
}

}


