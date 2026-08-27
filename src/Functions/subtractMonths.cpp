#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMonths = FunctionDateOrDateTimeAddInterval<SubtractMonthsImpl>;

REGISTER_FUNCTION(SubtractMonths)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of months from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractMonths(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to subtract specified number of months from.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of months to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` months", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Subtract months from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMonths(date, 1) AS subtract_months_with_date,
    subtractMonths(date_time, 1) AS subtract_months_with_date_time,
    subtractMonths(date_time_string, 1) AS subtract_months_with_date_time_string
        )",
        R"(
┌─subtract_months_with_date─┬─subtract_months_with_date_time─┬─subtract_months_with_date_time_string─┐
│                2023-12-01 │            2023-12-01 00:00:00 │               2023-12-01 00:00:00.000 │
└───────────────────────────┴────────────────────────────────┴───────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 month)
        )",
        R"(
┌─minus(CAST('⋯lMonth(10))─┐
│               1997-08-16 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractMonths>(documentation);
}

}


