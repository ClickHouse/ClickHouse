#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFiveMinutes)
{
    FunctionDocumentation::Description description_to_start_of_five_minutes = R"(
Rounds down a date with time to the start of the nearest five-minute interval.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_five_minutes = R"(
toStartOfFiveMinutes(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_five_minutes = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_five_minutes = {"Returns the date with time rounded to the start of the nearest five-minute interval", {"DateTime"}};
    FunctionDocumentation::Examples examples_to_start_of_five_minutes = {
        {"Example", R"(
SELECT
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
        )", R"(
Row 1:
──────
toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_five_minutes = {22, 6};
    FunctionDocumentation::Category category_to_start_of_five_minutes = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_five_minutes = {
        description_to_start_of_five_minutes,
        syntax_to_start_of_five_minutes,
        arguments_to_start_of_five_minutes,
        returned_value_to_start_of_five_minutes,
        examples_to_start_of_five_minutes,
        introduced_in_to_start_of_five_minutes,
        category_to_start_of_five_minutes
    };

    factory.registerFunction<FunctionToStartOfFiveMinutes>(documentation_to_start_of_five_minutes);
    factory.registerAlias("toStartOfFiveMinute", FunctionToStartOfFiveMinutes::name);
}

}


