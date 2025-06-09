#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfISOYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfISOYearImpl>;

REGISTER_FUNCTION(ToStartOfISOYear)
{
    FunctionDocumentation::Description description_to_start_of_iso_year = R"(
Rounds down a date or date with time to the first day of the ISO year, which can be different than a regular year. See [ISO week date](https://en.wikipedia.org/wiki/ISO_week_date).

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_iso_year = R"(
toStartOfISOYear(value)
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_iso_year = {
        {"value", "The date or date with time to round down to the first day of the ISO year. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_iso_year =
"Returns the first day of the ISO year for the given date or date with time. [`Date`](../data-types/date.md).";
    FunctionDocumentation::Examples examples_to_start_of_iso_year = {
        {"Round down to the first day of the ISO year", R"(
SELECT toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-01-02 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_iso_year = {1, 1};
    FunctionDocumentation::Category category_to_start_of_iso_year = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_iso_year = {
        description_to_start_of_iso_year,
        syntax_to_start_of_iso_year,
        arguments_to_start_of_iso_year,
        returned_value_to_start_of_iso_year,
        examples_to_start_of_iso_year,
        introduced_in_to_start_of_iso_year,
        category_to_start_of_iso_year
    };

    factory.registerFunction<FunctionToStartOfISOYear>(documentation_to_start_of_iso_year);
}

}


