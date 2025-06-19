#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourImpl>;

REGISTER_FUNCTION(ToStartOfHour)
{
    FunctionDocumentation::Description description_to_start_of_hour = R"(
Rounds down a date with time to the start of the hour.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_hour = R"(
toStartOfHour(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_hour = {
        {"datetime", "A date with time to convert.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_hour =
        {"Returns the date with time rounded down to the start of the hour.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_to_start_of_hour = {
        {"Round down to the start of the hour", R"(
SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30'));
    )", R"(
┌─────────────────res─┬─toTypeName(res)─┐
│ 2023-04-21 10:00:00 │ DateTime        │
└─────────────────────┴─────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_hour = {1, 1};
    FunctionDocumentation::Category category_to_start_of_hour = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_hour = {
        description_to_start_of_hour,
        syntax_to_start_of_hour,
        arguments_to_start_of_hour,
        returned_value_to_start_of_hour,
        examples_to_start_of_hour,
        introduced_in_to_start_of_hour,
        category_to_start_of_hour
    };

    factory.registerFunction<FunctionToStartOfHour>(documentation_to_start_of_hour);
}

}


