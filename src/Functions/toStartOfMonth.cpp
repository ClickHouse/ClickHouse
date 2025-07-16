#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToStartOfMonthImpl>;

REGISTER_FUNCTION(ToStartOfMonth)
{
    FunctionDocumentation::Description description_to_start_of_month = R"(
Rounds down a date or date with time to the first day of the month.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_month = R"(
toStartOfMonth(value)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_month = {
        {"value", "The date or date with time to round down to the first day of the month. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_month =
"Returns the first day of the month for the given date or date with time. [`Date`](../data-types/date.md).";
    FunctionDocumentation::Examples examples_to_start_of_month = {
        {"Round down to the first day of the month", R"(
SELECT toStartOfMonth(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                        2023-04-01 │
└───────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_month = {1, 1};
    FunctionDocumentation::Category category_to_start_of_month = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_month = {
        description_to_start_of_month,
        syntax_to_start_of_month,
        arguments_to_start_of_month,
        returned_value_to_start_of_month,
        examples_to_start_of_month,
        introduced_in_to_start_of_month,
        category_to_start_of_month
    };

    factory.registerFunction<FunctionToStartOfMonth>(documentation_to_start_of_month);
}

}


