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
    FunctionDocumentation::Description description_toWeek = R"(
This function returns the week number for date or datetime. The two-argument form of `toWeek()` enables you to specify whether the week starts
on Sunday or Monday and whether the return value should be in the range from `0` to `53` or from `1` to `53`.

[`toISOWeek()`](#toWeek) is a compatibility function that is equivalent to `toWeek(date,3)`.

The following table describes how the mode argument works.

| Mode | First day of week | Range | Week 1 is the first week ...    |
|------|-------------------|-------|---------------------------------|
| 0    | Sunday            | 0-53  | with a Sunday in this year      |
| 1    | Monday            | 0-53  | with 4 or more days this year   |
| 2    | Sunday            | 1-53  | with a Sunday in this year      |
| 3    | Monday            | 1-53  | with 4 or more days this year   |
| 4    | Sunday            | 0-53  | with 4 or more days this year   |
| 5    | Monday            | 0-53  | with a Monday in this year      |
| 6    | Sunday            | 1-53  | with 4 or more days this year   |
| 7    | Monday            | 1-53  | with a Monday in this year      |
| 8    | Sunday            | 1-53  | contains January 1              |
| 9    | Monday            | 1-53  | contains January 1              |

For mode values with a meaning of "with 4 or more days this year," weeks are numbered according to ISO 8601:1988:

- If the week containing January 1 has 4 or more days in the new year, it is week 1.
- Otherwise, it is the last week of the previous year, and the next week is week 1.

For mode values with a meaning of "contains January 1", the week contains January 1 is week 1.
It does not matter how many days in the new year the week contained, even if it contained only one day.
I.e. if the last week of December contains January 1 of the next year, it will be week 1 of the next year.

The first argument can also be specified as [`String`](../data-types/string.md) in a format supported by [`parseDateTime64BestEffort()`](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.
    )";
    FunctionDocumentation::Syntax syntax_toWeek = R"(
toWeek(datetime[, mode[, time_zone]])
    )";
    FunctionDocumentation::Arguments arguments_toWeek = {
        {"datetime", "Date or date with time to get the week number from.", {"Date", "DateTime"}},
        {"mode", "Optional. A mode `0` to `9` determines the first day of the week and the range of the week number. Default `0`."},
        {"time_zone", "Optional. Time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toWeek = {"Returns the week number according to the specified mode.", {"UInt32"}};
    FunctionDocumentation::Examples examples_toWeek = {
        {"Get week numbers with different modes", R"(
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9
        )",
        R"(
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toWeek = {20, 1};
    FunctionDocumentation::Category category_toWeek = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toWeek = {description_toWeek, syntax_toWeek, arguments_toWeek, returned_value_toWeek, examples_toWeek, introduced_in_toWeek, category_toWeek};

    factory.registerFunction<FunctionToWeek>(documentation_toWeek);

    FunctionDocumentation::Description description_toYearWeek = R"(
Returns the year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works like the mode argument of [`toWeek()`](/sql-reference/functions/date-time-functions#toWeek).

Warning: The week number returned by `toYearWeek()` can be different from what the `toWeek()` returns. `toWeek()` always returns week number in the context of the given year, and in case `toWeek()` returns `0`, `toYearWeek()` returns the value corresponding to the last week of previous year. See `prev_yearWeek` in example below.

The first argument can also be specified as [`String`](../data-types/string.md) in a format supported by [`parseDateTime64BestEffort()`](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.
    )";
    FunctionDocumentation::Syntax syntax_toYearWeek = R"(
toYearWeek(datetime[, mode[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments_toYearWeek = {
        {"datetime", "Date or date with time to get the year and week of.", {"Date", "DateTime"}},
        {"mode", "Optional. A mode `0` to `9` determines the first day of the week and the range of the week number. Default `0`."},
        {"timezone", "Optional. Time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toYearWeek = {"Returns year and week number as a combined integer value.", {"UInt32"}};
    FunctionDocumentation::Examples examples_toYearWeek = {
        {"Get year-week combinations with different modes", R"(
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9, toYearWeek(toDate('2022-01-01')) AS prev_yearWeek
        )",
        R"(
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┬─prev_yearWeek─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │        202152 │
└────────────┴───────────┴───────────┴───────────┴───────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toYearWeek = {20, 1};
    FunctionDocumentation::Category category_toYearWeek = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toYearWeek = {description_toYearWeek, syntax_toYearWeek, arguments_toYearWeek, returned_value_toYearWeek, examples_toYearWeek, introduced_in_toYearWeek, category_toYearWeek};

    factory.registerFunction<FunctionToYearWeek>(documentation_toYearWeek);

    FunctionDocumentation::Description description_to_start_of_week = R"(
Rounds a date or date with time down to the nearest Sunday or Monday.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
:::
    )";
    FunctionDocumentation::Syntax syntax_to_start_of_week = R"(
toStartOfWeek(datetime[, mode[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments_to_start_of_week =
    {
        {"datetime", "A date or date with time to convert.", {"Date", "DateTime", "Date32", "DateTime64"}},
        {"mode", "Determines the first day of the week as described in the `toWeek()` function. Default `0`.", {"UInt8"}},
        {"timezone", "The timezone to use for the conversion. If not specified, the server's timezone is used.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_start_of_week = {"Returns the date of the nearest Sunday or Monday on, or prior to, the given date, depending on the mode", {"Date", "Date32", "DateTime", "DateTime64"}};
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
    FunctionDocumentation documentation_to_start_of_week = {description_to_start_of_week, syntax_to_start_of_week, arguments_to_start_of_week, returned_value_to_start_of_week, examples_to_start_of_week, introduced_in_to_start_of_week, category_to_start_of_week};
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
        {"datetime", "A date or date with time to convert.", {"Date", "DateTime", "Date32", "DateTime64"}},
        {"mode", "Determines the first day of the week as described in the `toWeek()` function. Default `0`.", {"UInt8"}},
        {"timezone", "Optional. The timezone to use for the conversion. If not specified, the server's timezone is used.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_last_day_of_week = {"Returns the date of the nearest Saturday or Sunday, on or after the given date, depending on the mode", {"Date", "Date32", "DateTime", "DateTime64"}};
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
    FunctionDocumentation documentation_to_last_day_of_week = {description_to_last_day_of_week, syntax_to_last_day_of_week, arguments_to_last_day_of_week, returned_value_to_last_day_of_week, examples_to_last_day_of_week, introduced_in_to_last_day_of_week, category_to_last_day_of_week};
    factory.registerFunction<FunctionToLastDayOfWeek>(documentation_to_last_day_of_week);

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::Case::Insensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::Case::Insensitive);
}

}
