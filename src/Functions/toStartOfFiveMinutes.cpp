#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfFiveMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinutesImpl>;
using FunctionToStartOfFiveMinutesExtended = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfFiveMinutesExtendedImpl>;

REGISTER_FUNCTION(ToStartOfFiveMinutes)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the nearest five-minute interval.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfFiveMinutesExtended`](#toStartOfFiveMinutesExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfFiveMinutes(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the date with time rounded to the start of the nearest five-minute interval.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Example", R"(
SELECT
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
        )", R"(
Row 1:
──────
toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfFiveMinutes>(documentation);
    factory.registerAlias("toStartOfFiveMinute", FunctionToStartOfFiveMinutes::name);
}

REGISTER_FUNCTION(ToStartOfFiveMinutesExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfFiveMinutes`](#toStartOfFiveMinutes), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`DateTime64`](/sql-reference/data-types/datetime64) for `DateTime64` arguments, and `DateTime` for `DateTime` arguments. `Date` and `Date32` arguments are not supported.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfFiveMinutes`](#toStartOfFiveMinutes) function is monotonic only within the `DateTime` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfFiveMinutesExtended(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date with time rounded down to the start of the nearest five-minute interval.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and post-2106 five-minute bucket",
            R"(
SELECT toStartOfFiveMinutesExtended(toDateTime64('1969-12-31 23:57:30', 0, 'UTC')) AS pre_epoch,
       toStartOfFiveMinutesExtended(toDateTime64('2200-06-15 12:37:30', 0, 'UTC')) AS far_future
            )",
            R"(
┌───────────pre_epoch─┬──────────far_future─┐
│ 1969-12-31 23:55:00 │ 2200-06-15 12:35:00 │
└─────────────────────┴─────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfFiveMinutes` wraps for pre-`1970-01-01` input; `toStartOfFiveMinutesExtended` stays monotonic",
            R"(
SELECT toStartOfFiveMinutes(toDateTime64('1969-12-31 23:57:30', 0, 'UTC'))        AS narrow_wraps,
       toStartOfFiveMinutesExtended(toDateTime64('1969-12-31 23:57:30', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌────────narrow_wraps─┬─────────extended_ok─┐
│ 2106-02-07 06:23:16 │ 1969-12-31 23:55:00 │
└─────────────────────┴─────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfFiveMinutesExtended>(documentation);
    /// Parallel to the singular `toStartOfFiveMinute` alias on the narrow function.
    factory.registerAlias("toStartOfFiveMinuteExtended", FunctionToStartOfFiveMinutesExtended::name);
}

}


