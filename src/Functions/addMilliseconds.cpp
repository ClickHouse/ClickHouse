#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMilliseconds = FunctionDateOrDateTimeAddInterval<AddMillisecondsImpl>;

REGISTER_FUNCTION(AddMilliseconds)
{
    FunctionDocumentation::Description description = R"(
Adds a specified number of milliseconds to a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
addMilliseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date with time to add specified number of milliseconds to.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of milliseconds to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` plus `num` milliseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddMilliseconds>(documentation);
}

}


