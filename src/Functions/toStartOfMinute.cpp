#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfMinute = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfMinuteImpl>;
using FunctionToStartOfMinuteExtended = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfMinuteExtendedImpl>;

REGISTER_FUNCTION(ToStartOfMinute)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the minute.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfMinuteExtended`](#toStartOfMinuteExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfMinute(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the date with time rounded down to the start of the minute.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the start of the minute", R"(
SELECT
    toStartOfMinute(toDateTime('2023-04-21 10:20:30')),
    toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8))
FORMAT Vertical
    )", R"(
Row 1:
──────
toStartOfMinute(toDateTime('2023-04-21 10:20:30')):           2023-04-21 10:20:00
toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8)): 2023-04-21 10:20:00
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfMinute>(documentation);
}

REGISTER_FUNCTION(ToStartOfMinuteExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfMinute`](#toStartOfMinute), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`DateTime64`](/sql-reference/data-types/datetime64) for `DateTime64` arguments, and `DateTime` for `DateTime` arguments. `Date` and `Date32` arguments are not supported.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfMinute`](#toStartOfMinute) function is monotonic only within the `DateTime` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfMinuteExtended(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date with time rounded down to the start of the minute.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and post-2106 start-of-minute",
            R"(
SELECT toStartOfMinuteExtended(toDateTime64('1969-12-31 23:59:30', 0, 'UTC')) AS pre_epoch,
       toStartOfMinuteExtended(toDateTime64('2200-06-15 12:34:56', 0, 'UTC')) AS far_future
            )",
            R"(
┌───────────pre_epoch─┬──────────far_future─┐
│ 1969-12-31 23:59:00 │ 2200-06-15 12:34:00 │
└─────────────────────┴─────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfMinute` wraps for pre-`1970-01-01` input; `toStartOfMinuteExtended` stays monotonic",
            R"(
SELECT toStartOfMinute(toDateTime64('1969-12-31 23:59:30', 0, 'UTC'))        AS narrow_wraps,
       toStartOfMinuteExtended(toDateTime64('1969-12-31 23:59:30', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌────────narrow_wraps─┬─────────extended_ok─┐
│ 2106-02-07 06:27:16 │ 1969-12-31 23:59:00 │
└─────────────────────┴─────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfMinuteExtended>(documentation);
}

}


