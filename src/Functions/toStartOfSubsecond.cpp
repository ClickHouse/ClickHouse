#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfMillisecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMillisecondImpl>;

REGISTER_FUNCTION(ToStartOfMillisecond)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the milliseconds.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfMillisecond(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Input value with sub-milliseconds.", { "DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfMillisecond>(documentation);
}

using FunctionToStartOfMicrosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfMicrosecondImpl>;

REGISTER_FUNCTION(ToStartOfMicrosecond)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the microseconds.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfMicrosecond(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Input value with sub-microseconds", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfMicrosecond>(documentation);
}

using FunctionToStartOfNanosecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfNanosecondImpl>;

REGISTER_FUNCTION(ToStartOfNanosecond)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the nanoseconds.
        )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfNanosecond(datetime[, timezone])
        )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date and time.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Input value with nanoseconds.", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfNanosecond>(documentation);
}

}
