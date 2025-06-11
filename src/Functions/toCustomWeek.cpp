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

`toISOWeek()` is a compatibility function that is equivalent to `toWeek(date,3)`.

The following table describes how the mode argument works.

| Mode | First day of week | Range | Week 1 is the first week ...    |
|------|-------------------|-------|-------------------------------|
| 0    | Sunday            | 0-53  | with a Sunday in this year    |
| 1    | Monday            | 0-53  | with 4 or more days this year |
| 2    | Sunday            | 1-53  | with a Sunday in this year    |
| 3    | Monday            | 1-53  | with 4 or more days this year |
| 4    | Sunday            | 0-53  | with 4 or more days this year |
| 5    | Monday            | 0-53  | with a Monday in this year    |
| 6    | Sunday            | 1-53  | with 4 or more days this year |
| 7    | Monday            | 1-53  | with a Monday in this year    |
| 8    | Sunday            | 1-53  | contains January 1            |
| 9    | Monday            | 1-53  | contains January 1            |

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
        {"datetime", "Date or date with time to get the week number from. [`Date`](/sql-reference/data-types/date)/[`DateTime`](/sql-reference/data-types/datetime)."},
        {"mode", "Optional. A mode 0 to 9 determines the first day of the week and the range of the week number. Default `0`."},
        {"time_zone", "Optional. Time zone. [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toWeek = "Returns the week number according to the specified mode. [`UInt32`](/sql-reference/data-types/int-uint).";
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
    FunctionDocumentation documentation_toWeek = {
        description_toWeek,
        syntax_toWeek,
        arguments_toWeek,
        returned_value_toWeek,
        examples_toWeek,
        introduced_in_toWeek,
        category_toWeek
    };

    factory.registerFunction<FunctionToWeek>(documentation_toWeek);

    FunctionDocumentation::Description description_toYearWeek = R"(
Returns the year and week for a date. The year in the result may be different from the year in the date argument for the first and the last week of the year.

The mode argument works like the mode argument of [`toWeek()`](/sql-reference/functions/date-time-functions#toWeek).

`toISOYear()` is a compatibility function that is equivalent to `intDiv(toYearWeek(date,3),100)`.

Warning: The week number returned by `toYearWeek()` can be different from what the `toWeek()` returns. `toWeek()` always returns week number in the context of the given year, and in case `toWeek()` returns `0`, `toYearWeek()` returns the value corresponding to the last week of previous year. See `prev_yearWeek` in example below.

The first argument can also be specified as [`String`](../data-types/string.md) in a format supported by [`parseDateTime64BestEffort()`](type-conversion-functions.md#parsedatetime64besteffort). Support for string arguments exists only for reasons of compatibility with MySQL which is expected by certain 3rd party tools. As string argument support may in future be made dependent on new MySQL-compatibility settings and because string parsing is generally slow, it is recommended to not use it.
    )";
    FunctionDocumentation::Syntax syntax_toYearWeek = R"(
toYearWeek(datetime[, mode[, timezone]])
    )";
    FunctionDocumentation::Arguments arguments_toYearWeek = {
        {"datetime", "Date or date with time to get the year and week of. [`Date`](/sql-reference/data-types/date)/[`DateTime`](/sql-reference/data-types/datetime)."},
        {"mode", "Optional. A mode 0 to 9 determines the first day of the week and the range of the week number. Default `0`."},
        {"timezone", "Optional. Time zone. [`String`](/sql-reference/data-types/string)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_toYearWeek = "Returns year and week number as a combined integer value. [`UInt32`](/sql-reference/data-types/int-uint).";
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
    FunctionDocumentation documentation_toYearWeek = {
        description_toYearWeek,
        syntax_toYearWeek,
        arguments_toYearWeek,
        returned_value_toYearWeek,
        examples_toYearWeek,
        introduced_in_toYearWeek,
        category_toYearWeek
    };

    factory.registerFunction<FunctionToYearWeek>(documentation_toYearWeek);

    factory.registerFunction<FunctionToStartOfWeek>();
    factory.registerFunction<FunctionToLastDayOfWeek>();

    /// Compatibility aliases for mysql.
    factory.registerAlias("week", "toWeek", FunctionFactory::Case::Insensitive);
    factory.registerAlias("yearweek", "toYearWeek", FunctionFactory::Case::Insensitive);
}

}
