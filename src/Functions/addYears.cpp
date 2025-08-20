#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddYears = FunctionDateOrDateTimeAddInterval<AddYearsImpl>;

REGISTER_FUNCTION(AddYears)
{
    FunctionDocumentation::Description description = R"(
Adds a specified number of years to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
addYears(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date or date with time to add specified number of years to.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of years to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` plus `num` years", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Add years to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time,
    addYears(date_time_string, 1) AS add_years_with_date_time_string
        )",
        R"(
┌─add_years_with_date─┬─add_years_with_date_time─┬─add_years_with_date_time_string─┐
│          2025-01-01 │      2025-01-01 00:00:00 │         2025-01-01 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 year)
        )",
        R"(
┌─plus(CAST('1⋯alYear(10))─┐
│               2008-06-16 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddYears>(documentation);
}

}


