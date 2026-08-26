#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourImpl>;

REGISTER_FUNCTION(ToStartOfHour)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the hour.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/reference/settings/session-settings/enable#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfHour(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the date with time rounded down to the start of the hour.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the start of the hour", R"(
SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30'));
    )", R"(
┌─toStartOfHour(toDateTime('2023-04-21 10:20:30'))─┐
│                              2023-04-21 10:00:00 │
└──────────────────────────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfHour>(documentation);
}

}


