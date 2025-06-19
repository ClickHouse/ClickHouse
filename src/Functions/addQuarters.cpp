#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddQuarters = FunctionDateOrDateTimeAddInterval<AddQuartersImpl>;

REGISTER_FUNCTION(AddQuarters)
{
    FunctionDocumentation::Description description_addQuarters = R"(
Adds a specified number of quarters to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addQuarters = R"(
addQuarters(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addQuarters = {
        {"datetime", "Date or date with time to add specified number of quarters to.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of quarters to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addQuarters = {"Returns `datetime` plus `num` quarters", {"Date"}};
    FunctionDocumentation::Examples examples_addQuarters = {
        {"Add quarters to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addQuarters(date, 1) AS add_quarters_with_date,
    addQuarters(date_time, 1) AS add_quarters_with_date_time,
    addQuarters(date_time_string, 1) AS add_quarters_with_date_time_string
        )",
        R"(
┌─add_quarters_with_date─┬─add_quarters_with_date_time─┬─add_quarters_with_date_time_string─┐
│             2024-04-01 │         2024-04-01 00:00:00 │            2024-04-01 00:00:00.000 │
└────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 quarter)
        )",
        R"(
┌─plus(CAST('1⋯uarter(10))─┐
│               2000-12-16 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addQuarters = {20, 1};
    FunctionDocumentation::Category category_addQuarters = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addQuarters = {
        description_addQuarters,
        syntax_addQuarters,
        arguments_addQuarters,
        returned_value_addQuarters,
        examples_addQuarters,
        introduced_in_addQuarters,
        category_addQuarters
    };

    factory.registerFunction<FunctionAddQuarters>(documentation_addQuarters);
}

}


