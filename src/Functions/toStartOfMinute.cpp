#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfMinute = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfMinuteImpl>;

REGISTER_FUNCTION(ToStartOfMinute)
{
    FunctionDocumentation::Description description_to_start_of_minute = R"(
Rounds down a date with time to the start of the minute.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_minute = R"(
toStartOfMinute(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_minute = {
        {"datetime", "A date with time to convert.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_minute =
        "Returns the date with time rounded down to the start of the minute. [`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_to_start_of_minute = {
        {"Round down to the start of the minute", R"(
SELECT
    toStartOfMinute(toDateTime('2023-04-21 10:20:30')),
    toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8))
FORMAT Vertical
    )", R"(
Row 1:
──────
toStartOfMinute(toDateTime('2023-04-21 10:20:30')):           2023-04-21 10:20:00
toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8)): 2023-04-21 10:20:00
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_minute = {1, 1};
    FunctionDocumentation::Category category_to_start_of_minute = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_minute = {
        description_to_start_of_minute,
        syntax_to_start_of_minute,
        arguments_to_start_of_minute,
        returned_value_to_start_of_minute,
        examples_to_start_of_minute,
        introduced_in_to_start_of_minute,
        category_to_start_of_minute
    };

    factory.registerFunction<FunctionToStartOfMinute>(documentation_to_start_of_minute);
}

}


