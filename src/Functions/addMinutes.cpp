#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMinutes = FunctionDateOrDateTimeAddInterval<AddMinutesImpl>;

REGISTER_FUNCTION(AddMinutes)
{
    FunctionDocumentation::Description description = R"(
Adds a specified number of minutes to a date, a date with time or a string-encoded date or date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
addMinutes(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date or date with time to add specified number of minutes to.", {"Date", "Date32", "DateTime", "DateTime64", "String"}},
        {"num", "Number of minutes to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` plus `num` minutes", {"DateTime", "DateTime64(3)"}};
    FunctionDocumentation::Examples examples = {
        {"Add minutes to different date types", R"(
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMinutes(date, 20) AS add_minutes_with_date,
    addMinutes(date_time, 20) AS add_minutes_with_date_time,
    addMinutes(date_time_string, 20) AS add_minutes_with_date_time_string
        )",
        R"(
┌─add_minutes_with_date─┬─add_minutes_with_date_time─┬─add_minutes_with_date_time_string─┐
│   2024-01-01 00:20:00 │        2024-01-01 00:20:00 │           2024-01-01 00:20:00.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::Date, INTERVAL 10 minute)
        )",
        R"(
┌─plus(CAST('1⋯Minute(10))─┐
│      1998-06-16 00:10:00 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddMinutes>(documentation);
}

}


