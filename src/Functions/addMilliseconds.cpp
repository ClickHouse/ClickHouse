#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMilliseconds = FunctionDateOrDateTimeAddInterval<AddMillisecondsImpl>;

REGISTER_FUNCTION(AddMilliseconds)
{
    FunctionDocumentation::Description description_addMilliseconds = R"(
Adds a specified number of milliseconds to a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax_addMilliseconds = R"(
addMilliseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_addMilliseconds = {
        {"datetime", "Date with time to add specified number of milliseconds to.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of milliseconds to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_addMilliseconds = {"Returns `datetime` plus `num` milliseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples_addMilliseconds = {
        {"Add milliseconds to different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMilliseconds(date_time, 1000) AS add_milliseconds_with_date_time,
    addMilliseconds(date_time_string, 1000) AS add_milliseconds_with_date_time_string
        )",
        R"(
┌─add_milliseconds_with_date_time─┬─add_milliseconds_with_date_time_string─┐
│         2024-01-01 00:00:01.000 │                2024-01-01 00:00:01.000 │
└─────────────────────────────────┴────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::DateTime, INTERVAL 10 millisecond)
        )",
        R"(
┌─plus(CAST('1⋯second(10))─┐
│  1998-06-16 00:00:00.010 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_addMilliseconds = {22, 6};
    FunctionDocumentation::Category category_addMilliseconds = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_addMilliseconds = {
        description_addMilliseconds,
        syntax_addMilliseconds,
        arguments_addMilliseconds,
        returned_value_addMilliseconds,
        examples_addMilliseconds,
        introduced_in_addMilliseconds,
        category_addMilliseconds
    };

    factory.registerFunction<FunctionAddMilliseconds>(documentation_addMilliseconds);
}

}


