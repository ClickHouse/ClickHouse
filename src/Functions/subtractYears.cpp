#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractYears = FunctionDateOrDateTimeAddInterval<SubtractYearsImpl>;

REGISTER_FUNCTION(SubtractYears)
{
    FunctionDocumentation::Description description_subtractYears = R"(
Subtracts a specified number of years from a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractYears = R"(
subtractYears(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractYears =
    {
        {"datetime", "Date or date with time to subtract specified number of years from.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of years to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractYears = {"Returns `datetime` minus `num` years", {"Date", "Date32", "DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_subtractYears = {
        {"Subtract years from different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time,
    subtractYears(date_time_string, 1) AS subtract_years_with_date_time_string
        )",
        R"(
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┬─subtract_years_with_date_time_string─┐
│               2023-01-01 │           2023-01-01 00:00:00 │              2023-01-01 00:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::Date, INTERVAL 10 year)
        )",
        R"(
┌─minus(CAST('⋯alYear(10))─┐
│               1988-06-16 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractYears = {1, 1};
    FunctionDocumentation::Category category_subtractYears = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractYears = {
        description_subtractYears,
        syntax_subtractYears,
        arguments_subtractYears,
        returned_value_subtractYears,
        examples_subtractYears,
        introduced_in_subtractYears,
        category_subtractYears
    };

    factory.registerFunction<FunctionSubtractYears>(documentation_subtractYears);
}

}


