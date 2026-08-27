#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfMonthImpl>;

REGISTER_FUNCTION(ToStartOfMonth)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the first day of the month.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfMonth(value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round down to the first day of the month.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the first day of the month for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the first day of the month", R"(
SELECT toStartOfMonth(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                        2023-04-01 │
└───────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfMonth>(documentation);
}

}


