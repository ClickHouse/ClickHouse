#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToMonday = FunctionDateOrDateTimeToDateOrDate32<ToMondayImpl>;

REGISTER_FUNCTION(ToMonday)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the Monday of the same week. Returns the date.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toMonday(value)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"value", "Date or date with time to round down to the Monday of the week.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the date of the Monday of the same week for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the Monday of the week", R"(
SELECT
toMonday(toDateTime('2023-04-21 10:20:30')), -- A Friday
toMonday(toDate('2023-04-24'));              -- Already a Monday
        )", R"(
┌─toMonday(toDateTime('2023-04-21 10:20:30'))─┬─toMonday(toDate('2023-04-24'))─┐
│                                  2023-04-17 │                     2023-04-24 │
└─────────────────────────────────────────────┴────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMonday>(documentation);
}

}


