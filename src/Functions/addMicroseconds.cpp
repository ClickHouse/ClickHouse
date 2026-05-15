#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionAddMicroseconds = FunctionDateOrDateTimeAddInterval<AddMicrosecondsImpl>;

REGISTER_FUNCTION(AddMicroseconds)
{
    FunctionDocumentation::Description description = R"(
Adds a specified number of microseconds to a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
addMicroseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date with time to add specified number of microseconds to.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of microseconds to add.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `date_time` plus `num` microseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Add microseconds to different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMicroseconds(date_time, 1000000) AS add_microseconds_with_date_time,
    addMicroseconds(date_time_string, 1000000) AS add_microseconds_with_date_time_string
        )",
        R"(
┌─add_microseconds_with_date_time─┬─add_microseconds_with_date_time_string─┐
│      2024-01-01 00:00:01.000000 │             2024-01-01 00:00:01.000000 │
└─────────────────────────────────┴────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateAdd('1998-06-16'::DateTime, INTERVAL 10 microsecond)
        )",
        R"(
┌─plus(CAST('19⋯osecond(10))─┐
│ 1998-06-16 00:00:00.000010 │
└────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionAddMicroseconds>(documentation);
}

}


