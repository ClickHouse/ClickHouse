#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToStartOfYear = FunctionDateOrDateTimeToDateOrDate32<ToStartOfYearImpl>;
using FunctionToStartOfYearExtended = FunctionDateOrDateTimeToDateOrDate32<ToStartOfYearExtendedImpl>;

REGISTER_FUNCTION(ToStartOfYear)
{
    FunctionDocumentation::Description description = R"(
Rounds down a date or date with time to the first day of the year. Returns the date as a `Date` object.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toStartOfYearExtended`](#toStartOfYearExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfYear(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round down.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the first day of the year for the given date/time", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round down to the first day of the year", R"(
SELECT toStartOfYear(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toStartOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                       2023-01-01 │
└──────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfYear>(documentation);
}

REGISTER_FUNCTION(ToStartOfYearExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toStartOfYear`](#toStartOfYear), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`Date32`](/sql-reference/data-types/date32) for `Date32` and `DateTime64` arguments, and `Date` for `Date` and `DateTime` arguments.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toStartOfYear`](#toStartOfYear) function is monotonic only within the `Date` range.
    )";
    FunctionDocumentation::Syntax syntax = "toStartOfYearExtended(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round down.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"First day of the year for the given date or date with time.", {"Date", "Date32"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and far-future first day of year",
            R"(
SELECT toStartOfYearExtended(toDateTime64('1960-06-15 12:00:00', 0, 'UTC')) AS pre_epoch,
       toStartOfYearExtended(toDate32('2200-11-15'))                        AS far_future
            )",
            R"(
┌──pre_epoch─┬─far_future─┐
│ 1960-01-01 │ 2200-01-01 │
└────────────┴────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toStartOfYear` wraps for pre-`1970-01-01` input; `toStartOfYearExtended` stays monotonic",
            R"(
SELECT toStartOfYear(toDateTime64('1969-06-15 12:00:00', 0, 'UTC'))        AS narrow_wraps,
       toStartOfYearExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│   2148-06-07 │  1969-01-01 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToStartOfYearExtended>(documentation);
}

}


