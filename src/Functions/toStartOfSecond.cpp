#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfSecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfSecondImpl>;

REGISTER_FUNCTION(ToStartOfSecond)
{
    FunctionDocumentation::Description description_to_start_of_second = R"(
Rounds down a date with time to the start of the seconds.
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_second = R"(
toStartOfSecond(datetime, [timezone])
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_second = {
        {"datetime", "Date and time to truncate sub-seconds from. [`DateTime64`](../data-types/datetime64.md)."},
        {"timezone", "[Timezone](../../operations/server-configuration-parameters/settings.md#timezone) for the returned value (optional). If not specified, the function uses the timezone of the `value` parameter. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_second =
        "Returns the input value without sub-seconds. [`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_to_start_of_second = {
        {"Query without timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64);
    )", R"(
┌───toStartOfSecond(dt64)─┐
│ 2020-01-01 10:20:30.000 │
└─────────────────────────┘
    )"},
        {"Query with timezone", R"(
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64, 'Asia/Istanbul');
    )", R"(
┌─toStartOfSecond(dt64, 'Asia/Istanbul')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_second = {20, 5};
    FunctionDocumentation::Category category_to_start_of_second = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_second = {
        description_to_start_of_second,
        syntax_to_start_of_second,
        arguments_to_start_of_second,
        returned_value_to_start_of_second,
        examples_to_start_of_second,
        introduced_in_to_start_of_second,
        category_to_start_of_second
    };

    factory.registerFunction<FunctionToStartOfSecond>(documentation_to_start_of_second);
}

}
