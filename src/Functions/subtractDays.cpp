#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractDays = FunctionDateOrDateTimeAddInterval<SubtractDaysImpl>;

REGISTER_FUNCTION(SubtractDays)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of days from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractDays(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to subtract specified number of days from.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of days to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` days", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractDays>(documentation);
}

}


