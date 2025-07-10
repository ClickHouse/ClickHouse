#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayImpl>;

REGISTER_FUNCTION(ToStartOfDay)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the day.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfDay(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date or date with time to round.", {"Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the date with time rounded down to the start of the day.", {"Date", "DateTime", "Date32", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the start of the day", R"(
SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'))
    )", R"(
┌─toStartOfDay(toDateTime('2023-04-21 10:20:30'))─┐
│                             2023-04-21 00:00:00 │
└─────────────────────────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfDay>(documentation);
}

}


