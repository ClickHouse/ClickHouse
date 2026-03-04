#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractMicroseconds = FunctionDateOrDateTimeAddInterval<SubtractMicrosecondsImpl>;
REGISTER_FUNCTION(SubtractMicroseconds)
{
    FunctionDocumentation::Description description = R"(
Subtracts a specified number of microseconds from a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax = R"(
subtractMicroseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to subtract specified number of microseconds from.", {"DateTime", "DateTime64", "String"}},
        {"num", "Number of microseconds to subtract.", {"(U)Int*", "Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `datetime` minus `num` microseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Subtract microseconds from different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMicroseconds(date_time, 1000000) AS subtract_microseconds_with_date_time,
    subtractMicroseconds(date_time_string, 1000000) AS subtract_microseconds_with_date_time_string
        )",
        R"(
┌─subtract_microseconds_with_date_time─┬─subtract_microseconds_with_date_time_string─┐
│           2023-12-31 23:59:59.000000 │                  2023-12-31 23:59:59.000000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::DateTime, INTERVAL 10 microsecond)
        )",
        R"(
┌─minus(CAST('1⋯osecond(10))─┐
│ 1998-06-15 23:59:59.999990 │
└────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSubtractMicroseconds>(documentation);
}

}


