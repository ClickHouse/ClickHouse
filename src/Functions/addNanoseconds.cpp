#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddNanoseconds = FunctionDateOrDateTimeAddInterval<AddNanosecondsImpl>;

REGISTER_FUNCTION(AddNanoseconds)
{
    FunctionDocumentation::Description description = R"(
Adds a specified number of nanoseconds to a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
addNanoseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date with time to add specified number of nanoseconds to.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of nanoseconds to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` plus `num` nanoseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Add nanoseconds to different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addNanoseconds(date_time, 1000) AS add_nanoseconds_with_date_time,
    addNanoseconds(date_time_string, 1000) AS add_nanoseconds_with_date_time_string
        )",
        R"(
┌─add_nanoseconds_with_date_time─┬─add_nanoseconds_with_date_time_string─┐
│  2024-01-01 00:00:00.000001000 │         2024-01-01 00:00:00.000001000 │
└────────────────────────────────┴───────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::DateTime, INTERVAL 1000 nanosecond)
        )",
        R"(
┌─plus(CAST('199⋯osecond(1000))─┐
│ 1998-06-16 00:00:00.000001000 │
└───────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddNanoseconds>(documentation);
}

}


