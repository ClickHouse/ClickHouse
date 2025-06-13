#include <DataTypes/DataTypesNumber.h>
#include <Functions/CustomWeekTransforms.h>
#include <Functions/FunctionCustomWeekToSomething.h>
#include <Functions/FunctionCustomWeekToDateOrDate32.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>


namespace DB
{
using FunctionToWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToWeekImpl>;
using FunctionToYearWeek = FunctionCustomWeekToSomething<DataTypeUInt32, ToYearWeekImpl>;
using FunctionToStartOfWeek = FunctionCustomWeekToDateOrDate32<ToStartOfWeekImpl>;
using FunctionToLastDayOfWeek = FunctionCustomWeekToDateOrDate32<ToLastDayOfWeekImpl>;

REGISTER_FUNCTION(ToCustomWeek)
{
    factory.registerFunction<FunctionToWeek>();
    factory.registerFunction<FunctionToYearWeek>();

    FunctionDocumentation::Description description_to_start_of_week = R"(
Rounds a date or date with time down to the nearest Sunday or Monday.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_week = R"(
toStartOfWeek(datetime[, mode[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_week = {
        {"datetime", "A date or date with time to convert. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`Date32`](../data-types/date32.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"mode", "Determines the first day of the week as described in the [`toWeek()`](#toweek) function. Default `0`."},
        {"timezone", "Optional. The timezone to use for the conversion. If not specified, the server's timezone is used. [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_week = "Returns the date of the nearest Sunday or Monday on, or prior to, the given date, depending on the mode. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`Date32`](../data-types/date32.md)/[`DateTime64`](../data-types/datetime64.md)..";
    FunctionDocumentation::Examples examples_to_start_of_week = {
        {"Round down to the nearest Sunday or Monday", R"(
    SELECT
        toStartOfWeek(toDateTime('2023-04-21 10:20:30')), /* a Friday */
        toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* a Friday */
        toStartOfWeek(toDate('2023-04-24')), /* a Monday */
        toStartOfWeek(toDate('2023-04-24'), 1) /* a Monday */
    FORMAT Vertical
    )", R"(
    Row 1:
    ──────
    toStartOfWeek(toDateTime('2023-04-21 10:20:30')):      2023-04-17
    toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1):   2023-04-17
    toStartOfWeek(toDate('2023-04-24')):                   2023-04-24
    toStartOfWeek(toDate('2023-04-24'), 1):                2023-04-24
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_start_of_week = {20, 1};
    FunctionDocumentation::Category category_to_start_of_week = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_start_of_week = {
        description_to_start_of_week,
        syntax_to_start_of_week,
        arguments_to_start_of_week,
        returned_value_to_start_of_week,
        examples_to_start_of_week,
        introduced_in_to_start_of_week,
        category_to_start_of_week
    };
    factory.registerFunction<FunctionToStartOfWeek>(documentation_to_start_of_week);

    FunctionDocumentation::Description description_to_last_day_of_week = R"(
Rounds a date or date with time up to the nearest Saturday or Sunday.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_last_day_of_week = R"(
    toLastDayOfWeek(datetime[, mode[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments_to_last_day_of_week = {
        {"datetime", "A date or date with time to convert. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`Date32`](../data-types/date32.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"mode", "Determines the first day of the week as described in the [`toWeek()`](#toweek) function. Default `0`."},
        {"timezone", "Optional. The timezone to use for the conversion. If not specified, the server's timezone is used. [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_last_day_of_week = "Returns the date of the nearest Saturday or Sunday, on or after the given date, depending on the mode. [`Date`](../data-types/date.md)/[`DateTime`](../data-types/datetime.md)/[`Date32`](../data-types/date32.md)/[`DateTime64`](../data-types/datetime64.md).";
    FunctionDocumentation::Examples examples_to_last_day_of_week = {
        {"Round up to the nearest Saturday or Sunday", R"(
SELECT
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')), /* a Friday */
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* a Friday */
    toLastDayOfWeek(toDate('2023-04-23')), /* a Sunday */
    toLastDayOfWeek(toDate('2023-04-23'), 1) /* a Sunday */
FORMAT Vertical
    )", R"(
Row 1:
──────
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')):      2023-04-23
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1):   2023-04-22
toLastDayOfWeek(toDate('2023-04-23')):                   2023-04-23
toLastDayOfWeek(toDate('2023-04-23'), 1):                2023-04-23
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_last_day_of_week = {23, 5};
    FunctionDocumentation::Category category_to_last_day_of_week = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_last_day_of_week = {
        description_to_last_day_of_week,
        syntax_to_last_day_of_week,
        arguments_to_last_day_of_week,
        returned_value_to_last_day_of_week,
        examples_to_last_day_of_week,
        introduced_in_to_last_day_of_week,
        category_to_last_day_of_week
    };
    factory.registerFunction<FunctionToLastDayOfWeek>(documentation_to_last_day_of_week);

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::Case::Insensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::Case::Insensitive);
}

}
