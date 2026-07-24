#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfISOYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfISOYearImpl>;

REGISTER_FUNCTION(ToStartOfISOYear)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the first day of the ISO year, which can be different than a regular year. See [ISO week date](https://en.wikipedia.org/wiki/ISO_week_date).

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfISOYear(value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round down to the first day of the ISO year.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the first day of the ISO year for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the first day of the ISO year", R"(
SELECT toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-01-02 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfISOYear>(documentation);
}

}


