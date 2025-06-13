#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFifteenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFifteenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfFifteenMinutes)
{
    FunctionDocumentation::Description description_to_start_of_fifteen_minutes = R"(
Rounds down the date with time to the start of the fifteen-minute interval.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_fifteen_minutes = R"(
toStartOfFifteenMinutes(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_fifteen_minutes = 
    {
        {"datetime", "A [`DateTime`](../data-types/datetime.md) or [`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_fifteen_minutes = "Returns the date with time rounded to the start of the nearest fifteen-minute interval. [`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_to_start_of_fifteen_minutes = 
    {
        {"Example", R"(
SELECT
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
    )", R"(
Row 1:
──────
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:15:00
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_fifteen_minutes = {1, 1};
    FunctionDocumentation::Category category_to_start_of_fifteen_minutes = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_fifteen_minutes = 
    {
        description_to_start_of_fifteen_minutes,
        syntax_to_start_of_fifteen_minutes,
        arguments_to_start_of_fifteen_minutes,
        returned_value_to_start_of_fifteen_minutes,
        examples_to_start_of_fifteen_minutes,
        introduced_in_to_start_of_fifteen_minutes,
        category_to_start_of_fifteen_minutes
    };

    factory.registerFunction<FunctionToStartOfFifteenMinutes>(documentation_to_start_of_fifteen_minutes);
}

}


