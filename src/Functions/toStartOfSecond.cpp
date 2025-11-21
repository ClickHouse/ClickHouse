#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToStartOfSecond = FunctionDateOrDateTimeToSomething<DataTypeDateTime64, ToStartOfSecondImpl>;

REGISTER_FUNCTION(ToStartOfSecond)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the seconds.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfSecond(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date and time to truncate sub-seconds from.", {"DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value. If not specified, the function uses the timezone of the `value` parameter.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the input value without sub-seconds.", {"DateTime64"}};
    FunctionDocumentation::Examples examples = {
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
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfSecond>(documentation);
}

}
