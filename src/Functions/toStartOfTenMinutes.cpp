#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfTenMinutes = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfTenMinutesImpl>;
using FunctionToStartOfTenMinutesExtended = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfTenMinutesExtendedImpl>;

REGISTER_FUNCTION(ToStartOfTenMinutes)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the nearest ten-minute interval.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfTenMinutesExtended`](#toStartOfTenMinutesExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfTenMinutes(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the date with time rounded to the start of the nearest ten-minute interval.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Example", R"(
SELECT
    toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
        )", R"(
Row 1:
──────
toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:10:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfTenMinutes>(documentation);
}

REGISTER_FUNCTION(ToStartOfTenMinutesExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfTenMinutes`](#toStartOfTenMinutes), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`DateTime64`](/sql-reference/data-types/datetime64) for `DateTime64` arguments, and `DateTime` for `DateTime` arguments. `Date` and `Date32` arguments are not supported.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfTenMinutes`](#toStartOfTenMinutes) function is monotonic only within the `DateTime` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfTenMinutesExtended(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date with time rounded down to the start of the nearest ten-minute interval.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and post-2106 ten-minute bucket",
            R"(
SELECT toStartOfTenMinutesExtended(toDateTime64('1969-12-31 23:55:00', 0, 'UTC')) AS pre_epoch,
       toStartOfTenMinutesExtended(toDateTime64('2200-06-15 12:35:00', 0, 'UTC')) AS far_future
            )",
            R"(
┌───────────pre_epoch─┬──────────far_future─┐
│ 1969-12-31 23:50:00 │ 2200-06-15 12:30:00 │
└─────────────────────┴─────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfTenMinutes` wraps for pre-`1970-01-01` input; `toStartOfTenMinutesExtended` stays monotonic",
            R"(
SELECT toStartOfTenMinutes(toDateTime64('1969-12-31 23:55:00', 0, 'UTC'))        AS narrow_wraps,
       toStartOfTenMinutesExtended(toDateTime64('1969-12-31 23:55:00', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌────────narrow_wraps─┬─────────extended_ok─┐
│ 2106-02-07 06:18:16 │ 1969-12-31 23:50:00 │
└─────────────────────┴─────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfTenMinutesExtended>(documentation);
}

}


