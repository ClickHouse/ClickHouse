#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMilliseconds = FunctionDateOrDateTimeAddInterval<SubtractMillisecondsImpl>;
REGISTER_FUNCTION(SubtractMilliseconds)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of milliseconds from a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractMilliseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to subtract specified number of milliseconds from.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of milliseconds to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` milliseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Subtract milliseconds from different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMilliseconds(date_time, 1000) AS subtract_milliseconds_with_date_time,
    subtractMilliseconds(date_time_string, 1000) AS subtract_milliseconds_with_date_time_string
        )",
        R"(
┌─subtract_milliseconds_with_date_time─┬─subtract_milliseconds_with_date_time_string─┐
│              2023-12-31 23:59:59.000 │                     2023-12-31 23:59:59.000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::DateTime, INTERVAL 10 millisecond)
        )",
        R"(
┌─minus(CAST('⋯second(10))─┐
│  1998-06-15 23:59:59.990 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractMilliseconds>(documentation);
}

}


