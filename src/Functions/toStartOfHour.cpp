#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>


namespace DB
{

using FunctionToStartOfHour = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourImpl>;
using FunctionToStartOfHourExtended = FunctionDateOrDateTimeToDateTimeOrDateTime64<ToStartOfHourExtendedImpl>;

REGISTER_FUNCTION(ToStartOfHour)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date with time to the start of the hour.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfHourExtended`](#toStartOfHourExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toStartOfHour(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
        {"Returns the date with time rounded down to the start of the hour.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the start of the hour", R"(
SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30'));
    )", R"(
┌─────────────────res─┬─toTypeName(res)─┐
│ 2023-04-21 10:00:00 │ DateTime        │
└─────────────────────┴─────────────────┘
    )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfHour>(documentation);
}

REGISTER_FUNCTION(ToStartOfHourExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfHour`](#toStartOfHour), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`DateTime64`](/sql-reference/data-types/datetime64) for `DateTime64` arguments, and `DateTime` for `DateTime` arguments. `Date` and `Date32` arguments are not supported.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfHour`](#toStartOfHour) function is monotonic only within the `DateTime` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfHourExtended(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A date with time to round.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date with time rounded down to the start of the hour.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and post-2106 start-of-hour",
            R"(
SELECT toStartOfHourExtended(toDateTime64('1969-12-31 22:34:56', 0, 'UTC')) AS pre_epoch,
       toStartOfHourExtended(toDateTime64('2200-06-15 12:34:56', 0, 'UTC')) AS far_future
            )",
            R"(
┌───────────pre_epoch─┬──────────far_future─┐
│ 1969-12-31 22:00:00 │ 2200-06-15 12:00:00 │
└─────────────────────┴─────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfHour` wraps for pre-`1970-01-01` input; `toStartOfHourExtended` stays monotonic",
            R"(
SELECT toStartOfHour(toDateTime64('1969-12-31 22:34:56', 0, 'UTC'))        AS narrow_wraps,
       toStartOfHourExtended(toDateTime64('1969-12-31 22:34:56', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌────────narrow_wraps─┬─────────extended_ok─┐
│ 2106-02-07 04:28:16 │ 1969-12-31 22:00:00 │
└─────────────────────┴─────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfHourExtended>(documentation);
}

}


