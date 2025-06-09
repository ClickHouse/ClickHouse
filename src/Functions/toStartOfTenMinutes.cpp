#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfTenMinutesImpl>;

REGISTER_FUNCTION(ToStartOfTenMinutes)
{
    FunctionDocumentation::Description description_to_start_of_ten_minutes = R"(
Rounds down a date with time to the start of the nearest ten-minute interval.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_ten_minutes = R"(
toStartOfTenMinutes(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_ten_minutes = {
        {"datetime", "A [`DateTime`](../data-types/datetime.md) or [`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_ten_minutes = "Returns the date with time rounded to the start of the nearest ten-minute interval. [`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)..";
    FunctionDocumentation::Examples examples_to_start_of_ten_minutes = {
        {"Example", R"(
SELECT
    toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
        )", R"(
Row 1:
──────
toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:10:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_ten_minutes = {20, 1};
    FunctionDocumentation::Category category_to_start_of_ten_minutes = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_ten_minutes = {
        description_to_start_of_ten_minutes,
        syntax_to_start_of_ten_minutes,
        arguments_to_start_of_ten_minutes,
        returned_value_to_start_of_ten_minutes,
        examples_to_start_of_ten_minutes,
        introduced_in_to_start_of_ten_minutes,
        category_to_start_of_ten_minutes
    };

    factory.registerFunction<FunctionToStartOfTenMinutes>(documentation_to_start_of_ten_minutes);
}

}


