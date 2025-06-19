#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddHours = FunctionDateOrDateTimeAddInterval<AddHoursImpl>;

REGISTER_FUNCTION(AddHours)
{
    FunctionDocumentation::Description description_addHours = R"(
Adds a specified number of hours to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addHours = R"(
addHours(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addHours = {
        {"datetime", "Date or date with time to add specified number of hours to.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of hours to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addHours = {"Returns `datetime` plus `num` hours", {"DateTime"}};
    FunctionDocumentation::Examples examples_addHours = {
        {"Add hours to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addHours(date, 12) AS add_hours_with_date,
    addHours(date_time, 12) AS add_hours_with_date_time,
    addHours(date_time_string, 12) AS add_hours_with_date_time_string
        )",
        R"(
┌─add_hours_with_date─┬─add_hours_with_date_time─┬─add_hours_with_date_time_string─┐
│ 2024-01-01 12:00:00 │      2024-01-01 12:00:00 │         2024-01-01 12:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 hour)
        )",
        R"(
┌─plus(CAST('1⋯alHour(10))─┐
│      1998-06-16 10:00:00 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addHours = {1, 1};
    FunctionDocumentation::Category category_addHours = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addHours = {
        description_addHours,
        syntax_addHours,
        arguments_addHours,
        returned_value_addHours,
        examples_addHours,
        introduced_in_addHours,
        category_addHours
    };

    factory.registerFunction<FunctionAddHours>(documentation_addHours);
}

}


