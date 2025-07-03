#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToDateOrDate32<ToStartOfQuarterImpl>;

REGISTER_FUNCTION(ToStartOfQuarter)
{
    FunctionDocumentation::Description description_to_start_of_quarter = R"(
Rounds down a date or date with time to the first day of the quarter. The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_quarter = R"(
toStartOfQuarter(value)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_quarter = {
        {"value", "The date or date with time to round down to the first day of the quarter. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_quarter =
"Returns the first day of the quarter for the given date or date with time. [`Date`](../data-types/date.md).";
    FunctionDocumentation::Examples examples_to_start_of_quarter = {
        {"Round down to the first day of the quarter", R"(
SELECT toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-01 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_quarter = {1, 1};
    FunctionDocumentation::Category category_to_start_of_quarter = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_quarter = {
        description_to_start_of_quarter,
        syntax_to_start_of_quarter,
        arguments_to_start_of_quarter,
        returned_value_to_start_of_quarter,
        examples_to_start_of_quarter,
        introduced_in_to_start_of_quarter,
        category_to_start_of_quarter
    };

    factory.registerFunction<FunctionToStartOfQuarter>(documentation_to_start_of_quarter);
}

}


