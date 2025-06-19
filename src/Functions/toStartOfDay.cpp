#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayImpl>;

REGISTER_FUNCTION(ToStartOfDay)
{
    FunctionDocumentation::Description description_to_start_of_day = R"(
Rounds down a date with time to the start of the day.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_day = R"(
toStartOfDay(datetime)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_day = {
        {"datetime", "A date or date with time to convert.", {"Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_day =
        {"Returns the date with time rounded down to the start of the day.", {"Date", "DateTime", "Date32", "DateTime64"}};
    FunctionDocumentation::Examples examples_to_start_of_day = {
        {"Round down to the start of the day", R"(
SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'))
    )", R"(
┌─toStartOfDay(toDateTime('2023-04-21 10:20:30'))─┐
│                             2023-04-21 00:00:00 │
└─────────────────────────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_day = {1, 1};
    FunctionDocumentation::Category category_to_start_of_day = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_day = {
        description_to_start_of_day,
        syntax_to_start_of_day,
        arguments_to_start_of_day,
        returned_value_to_start_of_day,
        examples_to_start_of_day,
        introduced_in_to_start_of_day,
        category_to_start_of_day
    };

    factory.registerFunction<FunctionToStartOfDay>(documentation_to_start_of_day);
}

}


