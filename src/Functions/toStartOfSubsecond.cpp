#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMillisecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMillisecondImpl>;

REGISTER_FUNCTION(ToStartOfMillisecond)
{
    FunctionDocumentation::Description description_to_start_of_millisecond = R"(
Rounds down a date with time to the start of the milliseconds.
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_millisecond = R"(
toStartOfMillisecond(datetime, [timezone])
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_millisecond = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_millisecond = {"Input value with sub-milliseconds.", { "DateTime64"}};
    FunctionDocumentation::Examples examples_to_start_of_millisecond = {
        {"Query without timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMillisecond(dt64);
        )", R"(
┌────toStartOfMillisecond(dt64)─┐
│ 2020-01-01 10:20:30.999000000 │
└───────────────────────────────┘
        )"},
        {"Query with timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMillisecond(dt64, 'Asia/Istanbul');
        )", R"(
┌─toStartOfMillisecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999000000 │
└─────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_millisecond = {22, 6};
    FunctionDocumentation::Category category_to_start_of_millisecond = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_millisecond = {
        description_to_start_of_millisecond,
        syntax_to_start_of_millisecond,
        arguments_to_start_of_millisecond,
        returned_value_to_start_of_millisecond,
        examples_to_start_of_millisecond,
        introduced_in_to_start_of_millisecond,
        category_to_start_of_millisecond
    };
    factory.registerFunction<FunctionToStartOfMillisecond>();
}

using FunctionToStartOfMicrosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMicrosecondImpl>;

REGISTER_FUNCTION(ToStartOfMicrosecond)
{
    FunctionDocumentation::Description description_to_start_of_microsecond = R"(
Rounds down a date with time to the start of the microseconds.
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_microsecond = R"(
toStartOfMicrosecond(datetime, [timezone])
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_microsecond = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_microsecond = {"Input value with sub-microseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples_to_start_of_microsecond = {
        {"Query without timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64);
        )", R"(
┌────toStartOfMicrosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999000 │
└───────────────────────────────┘
        )"},
        {"Query with timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64, 'Asia/Istanbul');
        )", R"(
┌─toStartOfMicrosecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999999000 │
└─────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_microsecond = {22, 6};
    FunctionDocumentation::Category category_to_start_of_microsecond = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_microsecond = {
        description_to_start_of_microsecond,
        syntax_to_start_of_microsecond,
        arguments_to_start_of_microsecond,
        returned_value_to_start_of_microsecond,
        examples_to_start_of_microsecond,
        introduced_in_to_start_of_microsecond,
        category_to_start_of_microsecond
    };

    factory.registerFunction<FunctionToStartOfMicrosecond>(documentation_to_start_of_microsecond);
}

using FunctionToStartOfNanosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfNanosecondImpl>;

REGISTER_FUNCTION(ToStartOfNanosecond)
{
    FunctionDocumentation::Description description_to_start_of_nanosecond = R"(
Rounds down a date with time to the start of the nanoseconds.
        )";
    FunctionDocumentation::Syntax syntax_to_start_of_nanosecond = R"(
toStartOfNanosecond(datetime, [timezone])
        )";
    FunctionDocumentation::Arguments arguments_to_start_of_nanosecond = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_nanosecond = {"Input value with nanoseconds.", {"DateTime64"}};
    FunctionDocumentation::Examples examples_to_start_of_nanosecond = {
        {"Query without timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64);
        )", R"(
┌─────toStartOfNanosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999999 │
└───────────────────────────────┘
        )"},
        {"Query with timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64, 'Asia/Istanbul');
        )", R"(
┌─toStartOfNanosecond(dt64, 'Asia/Istanbul')─┐
│              2020-01-01 12:20:30.999999999 │
└────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_nanosecond = {22, 6};
    FunctionDocumentation::Category category_to_start_of_nanosecond = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_nanosecond = {
        description_to_start_of_nanosecond,
        syntax_to_start_of_nanosecond,
        arguments_to_start_of_nanosecond,
        returned_value_to_start_of_nanosecond,
        examples_to_start_of_nanosecond,
        introduced_in_to_start_of_nanosecond,
        category_to_start_of_nanosecond
    };

    factory.registerFunction<FunctionToStartOfNanosecond>(documentation_to_start_of_nanosecond);
}

}
