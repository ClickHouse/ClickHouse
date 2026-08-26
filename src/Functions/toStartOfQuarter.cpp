#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfQuarter = FunctionDateOrDateTimeToDateOrDate32<ToStartOfQuarterImpl>;

REGISTER_FUNCTION(ToStartOfQuarter)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the first day of the quarter. The first day of the quarter is either 1 January, 1 April, 1 July, or 1 October.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfQuarter(value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round down to the first day of the quarter.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the first day of the quarter for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the first day of the quarter", R"(
SELECT toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-01 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfQuarter>(documentation);
}

}


