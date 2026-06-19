#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfDay = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayImpl>;
using FunctionToStartOfDayExtended = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfDayExtendedImpl>;

REGISTER_FUNCTION(ToStartOfDay)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the day.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfDayExtended`](#toStartOfDayExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfDay(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date or date with time to round.", {"Date", "DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the date with time rounded down to the start of the day.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the start of the day", R"(
SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'))
    )", R"(
┌─toStartOfDay(toDateTime('2023-04-21 10:20:30'))─┐
│                             2023-04-21 00:00:00 │
└─────────────────────────────────────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfDay>(documentation);
}

REGISTER_FUNCTION(ToStartOfDayExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfDay`](#toStartOfDay), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`DateTime64`](/sql-reference/data-types/datetime64) for `Date`, `Date32`, and `DateTime64` arguments, and `DateTime` for `DateTime` arguments.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfDay`](#toStartOfDay) function is monotonic only within the `DateTime` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfDayExtended(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date or date with time to round.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date with time rounded down to the start of the day.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and post-2106 start-of-day",
            R"(
SELECT toStartOfDayExtended(toDateTime64('1969-12-31 22:00:00', 0, 'UTC')) AS pre_epoch,
       toStartOfDayExtended(toDateTime64('2200-06-15 12:34:56', 0, 'UTC')) AS far_future
            )",
            R"(
┌───────────pre_epoch─┬──────────far_future─┐
│ 1969-12-31 00:00:00 │ 2200-06-15 00:00:00 │
└─────────────────────┴─────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfDay` wraps for `Date` past `2106-02-07` (`Date` max 2149 exceeds `DateTime` max); `toStartOfDayExtended` stays monotonic",
            R"(
SELECT toStartOfDay(toDate('2149-06-06'))        AS narrow_wraps,
       toStartOfDayExtended(toDate('2149-06-06')) AS extended_ok
            )",
            R"(
┌────────narrow_wraps─┬─────────────extended_ok─┐
│ 2013-04-29 17:31:44 │ 2149-06-06 00:00:00.000 │
└─────────────────────┴─────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfDayExtended>(documentation);
}

}


