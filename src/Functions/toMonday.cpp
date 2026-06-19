#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToMonday = FunctionDateOrDateTimeToDateOrDate32<ToMondayImpl>;
using FunctionToMondayExtended = FunctionDateOrDateTimeToDateOrDate32<ToMondayExtendedImpl>;

REGISTER_FUNCTION(ToMonday)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the Monday of the same week. Returns the date.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toMondayExtended`](#toMondayExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toMonday(value)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"value", "Date or date with time to round down to the Monday of the week.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the date of the Monday of the same week for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the Monday of the week", R"(
SELECT
toMonday(toDateTime('2023-04-21 10:20:30')), -- A Friday
toMonday(toDate('2023-04-24'));              -- Already a Monday
        )", R"(
┌─toMonday(toDateTime('2023-04-21 10:20:30'))─┬─toMonday(toDate('2023-04-24'))─┐
│                                  2023-04-17 │                     2023-04-24 │
└─────────────────────────────────────────────┴────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMonday>(documentation);
}

REGISTER_FUNCTION(ToMondayExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toMonday`](#toMonday), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`Date32`](/sql-reference/data-types/date32) for `Date32` and `DateTime64` arguments, and `Date` for `Date` and `DateTime` arguments.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toMonday`](#toMonday) function is monotonic only within the `Date` range.
    )";
    FunctionDocumentation::Syntax syntax = "toMondayExtended(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Date or date with time to round down to the Monday of the week.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Date of the Monday of the same week for the given date or date with time.", {"Date", "Date32"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and far-future Monday",
            R"(
SELECT toMondayExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')) AS pre_epoch,
       toMondayExtended(toDate32('2200-06-15'))                        AS far_future
            )",
            R"(
┌──pre_epoch─┬─far_future─┐
│ 1969-06-09 │ 2200-06-09 │
└────────────┴────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toMonday` wraps for pre-`1970-01-01` input; `toMondayExtended` stays monotonic",
            R"(
SELECT toMonday(toDateTime64('1969-06-15 12:00:00', 0, 'UTC'))        AS narrow_wraps,
       toMondayExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│   2148-11-13 │  1969-06-09 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMondayExtended>(documentation);
}

}


