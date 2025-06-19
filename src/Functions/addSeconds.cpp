#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddSeconds = FunctionDateOrDateTimeAddInterval<AddSecondsImpl>;

REGISTER_FUNCTION(AddSeconds)
{
    FunctionDocumentation::Description description_addSeconds = R"(
Adds a specified number of seconds to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax_addSeconds = R"(
addSeconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addSeconds = {
        {"datetime", "Date or date with time to add specified number of seconds to.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of seconds to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addSeconds = {"Returns `datetime` plus `num` seconds", {"DateTime"}};
    FunctionDocumentation::Examples examples_addSeconds = {
        {"Add seconds to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addSeconds(date, 30) AS add_seconds_with_date,
    addSeconds(date_time, 30) AS add_seconds_with_date_time,
    addSeconds(date_time_string, 30) AS add_seconds_with_date_time_string
        )",
        R"(
┌─add_seconds_with_date─┬─add_seconds_with_date_time─┬─add_seconds_with_date_time_string─┐
│   2024-01-01 00:00:30 │        2024-01-01 00:00:30 │           2024-01-01 00:00:30.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 second)
        )",
        R"(
┌─dateAdd('1998-06-16'::Date, INTERVAL 10 second)─┐
│                             1998-06-16 00:00:10 │
└─────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addSeconds = {1, 1};
    FunctionDocumentation::Category category_addSeconds = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addSeconds = {
        description_addSeconds,
        syntax_addSeconds,
        arguments_addSeconds,
        returned_value_addSeconds,
        examples_addSeconds,
        introduced_in_addSeconds,
        category_addSeconds
    };

    factory.registerFunction<FunctionAddSeconds>(documentation_addSeconds);
}

}


