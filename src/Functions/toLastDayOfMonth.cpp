#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateOrDate32.h>


namespace DB
{

using FunctionToLastDayOfMonth = FunctionDateOrDateTimeToDateOrDate32<ToLastDayOfMonthImpl>;
using FunctionToLastDayOfMonthExtended = FunctionDateOrDateTimeToDateOrDate32<ToLastDayOfMonthExtendedImpl>;

REGISTER_FUNCTION(ToLastDayOfMonth)
{
    FunctionDocumentation::Description description = R"(
Rounds up a date or date with time to the last day of the month.

:::note
The return type can be configured by setting [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions).
For an always-on, per-function alternative independent of the session setting (recommended for primary key and partition expressions), see [`toLastDayOfMonthExtended`](#toLastDayOfMonthExtended).
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
toLastDayOfMonth(value)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round up to the last day of the month.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {"Returns the date of the last day of the month for the given date or date with time.", {"Date"}};
    FunctionDocumentation::Examples examples = {
        {"Round up to the last day of the month", R"(
SELECT toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))
        )", R"(
┌─toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-30 │
└─────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToLastDayOfMonth>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("LAST_DAY", "toLastDayOfMonth", FunctionFactory::Case::Insensitive);
}

REGISTER_FUNCTION(ToLastDayOfMonthExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toLastDayOfMonth`](#toLastDayOfMonth), but the result is monotonic across the entire range of every supported argument type, regardless of the [`enable_extended_results_for_datetime_functions`](/operations/settings/settings#enable_extended_results_for_datetime_functions) setting.

The result type is widened only as far as is needed to represent the result without wrapping. The function returns [`Date32`](/sql-reference/data-types/date32) for `Date32` and `DateTime64` arguments, and `Date` for `Date` and `DateTime` arguments.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toLastDayOfMonth`](#toLastDayOfMonth) function is monotonic only within the `Date` range.
    )";
    FunctionDocumentation::Syntax syntax = "toLastDayOfMonthExtended(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "The date or date with time to round up to the last day of the month.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Last day of the month for the given date or date with time.", {"Date", "Date32"}};
    FunctionDocumentation::Examples examples = {
        {
            "Pre-epoch and far-future last day of month",
            R"(
SELECT toLastDayOfMonthExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')) AS pre_epoch,
       toLastDayOfMonthExtended(toDate32('2200-02-15'))                        AS far_future
            )",
            R"(
┌──pre_epoch─┬─far_future─┐
│ 1969-06-30 │ 2200-02-28 │
└────────────┴────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toLastDayOfMonth` wraps for pre-`1970-01-01` input; `toLastDayOfMonthExtended` stays monotonic",
            R"(
SELECT toLastDayOfMonth(toDateTime64('1969-06-15 12:00:00', 0, 'UTC'))        AS narrow_wraps,
       toLastDayOfMonthExtended(toDateTime64('1969-06-15 12:00:00', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│   2148-12-04 │  1969-06-30 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToLastDayOfMonthExtended>(documentation);
}

}
