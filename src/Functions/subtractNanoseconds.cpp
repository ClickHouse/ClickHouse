#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>


namespace DB
{

using FunctionSubtractNanoseconds = FunctionDateOrDateTimeAddInterval<SubtractNanosecondsImpl>;

REGISTER_FUNCTION(SubtractNanoseconds)
{
    FunctionDocumentation::Description description_subtractNanoseconds = R"(
Subtracts a specified number of nanoseconds from a date with time or a string-encoded date with time.
    )";
    FunctionDocumentation::Syntax syntax_subtractNanoseconds = R"(
subtractNanoseconds(datetime, num)
    )";
    FunctionDocumentation::Arguments arguments_subtractNanoseconds = {
        {"datetime", "Date with time to subtract specified number of nanoseconds from. [`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)/[`String`](../data-types/string.md)."},
        {"num", "Number of nanoseconds to subtract. [`(U)Int*`](../data-types/int-uint.md)/[`Float*`](../data-types/float.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_subtractNanoseconds = "Returns `datetime` minus `num` nanoseconds. [`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_subtractNanoseconds = {
        {"Subtract nanoseconds from different date time types", R"(
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractNanoseconds(date_time, 1000) AS subtract_nanoseconds_with_date_time,
    subtractNanoseconds(date_time_string, 1000) AS subtract_nanoseconds_with_date_time_string
        )",
        R"(
┌─subtract_nanoseconds_with_date_time─┬─subtract_nanoseconds_with_date_time_string─┐
│       2023-12-31 23:59:59.999999000 │              2023-12-31 23:59:59.999999000 │
└─────────────────────────────────────┴────────────────────────────────────────────┘
        )"},
        {"Using alternative INTERVAL syntax", R"(
SELECT dateSub('1998-06-16'::DateTime, INTERVAL 10 nanosecond)
        )",
        R"(
┌─minus(CAST('19⋯anosecond(10))─┐
│ 1998-06-15 23:59:59.999999990 │
└───────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_subtractNanoseconds = {20, 1};
    FunctionDocumentation::Category category_subtractNanoseconds = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_subtractNanoseconds = {
        description_subtractNanoseconds,
        syntax_subtractNanoseconds,
        arguments_subtractNanoseconds,
        returned_value_subtractNanoseconds,
        examples_subtractNanoseconds,
        introduced_in_subtractNanoseconds,
        category_subtractNanoseconds
    };

    factory.registerFunction<FunctionSubtractNanoseconds>(documentation_subtractNanoseconds);
}

}


