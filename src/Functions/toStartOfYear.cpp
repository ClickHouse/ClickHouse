#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfYearImpl>;

REGISTER_FUNCTION(ToStartOfYear)
{
    FunctionDocumentation::Description description_to_start_of_year = R"(
Rounds down a date or date with time to the first day of the year. Returns the date as a `Date` object.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_year = "toStartOfYear(value)";
    FunctionDocumentation::Arguments arguments_to_start_of_year = {
        {"value", "The date or date with time to round down. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_year = "Returns the first day of the year for the given date/time. [`Date`](../data-types/date.md).";
    FunctionDocumentation::Examples examples_to_start_of_year = {
        {"Round down to the first day of the year", R"(
SELECT toStartOfYear(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                       2023-01-01 │
└──────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_year = {1, 1};
    FunctionDocumentation::Category category_to_start_of_year = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_year = {
        description_to_start_of_year,
        syntax_to_start_of_year,
        arguments_to_start_of_year,
        returned_value_to_start_of_year,
        examples_to_start_of_year,
        introduced_in_to_start_of_year,
        category_to_start_of_year
    };

    factory.registerFunction<FunctionToStartOfYear>(documentation_to_start_of_year);
}

}


