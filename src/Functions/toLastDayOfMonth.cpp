#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToLastDayOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToLastDayOfMonthImpl>;

REGISTER_FUNCTION(ToLastDayOfMonth)
{
    FunctionDocumentation::Description description = R"(
Rounds up a date or date with time to the last day of the month.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toLastDayOfMonth(value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round up to the last day of the month.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the date of the last day of the month for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round up to the last day of the month", R"(
SELECT toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-30 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToLastDayOfMonth>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("LAST_DAY", "toLastDayOfMonth", FunctionFactory::Case::Insensitive);
}

}
