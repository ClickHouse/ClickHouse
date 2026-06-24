#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeWeekNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeWeekNumImpl<ResultPrecision::Standard>>;
using FunctionToRelativeWeekNumExtended = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToRelativeWeekNumExtendedImpl>;

REGISTER_FUNCTION(ToRelativeWeekNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of weeks elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in weeks between two dates or dates with time, e.g., `toRelativeWeekNum(dt1) - toRelativeWeekNum(dt2)`.
For an always-on, signed `Int64` variant that is monotonic on cross-epoch input and recommended in primary key and partition expressions, see [`toRelativeWeekNumExtended`](#toRelativeWeekNumExtended).
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeWeekNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of weeks from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative week numbers", R"(
SELECT toRelativeWeekNum(toDate('2023-01-08')) - toRelativeWeekNum(toDate('2023-01-01')) AS weeks_difference
        )",
        R"(
┌─weeks_difference─┐
│                1 │
└──────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeWeekNum>(documentation);
}

REGISTER_FUNCTION(ToRelativeWeekNumExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toRelativeWeekNum`](#toRelativeWeekNum), but returns a signed [`Int64`](/sql-reference/data-types/int-uint) instead of `UInt32`.

The wider, signed return type represents the full range of [`Date32`](/sql-reference/data-types/date32) and [`DateTime64`](/sql-reference/data-types/datetime64) arguments. The narrow `UInt32` result wraps to a large positive value for timestamps before `1970-01-01`.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toRelativeWeekNum`](#toRelativeWeekNum) function is monotonic only for arguments at or after `1970-01-01`.
    )";
    FunctionDocumentation::Syntax syntax = "toRelativeWeekNumExtended(date)";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Number of weeks from a fixed reference point in the past, as a signed 64-bit integer.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Signed weeks across the epoch",
            R"(
SELECT toRelativeWeekNumExtended(toDate32('1969-12-21')) AS pre_epoch,
       toRelativeWeekNumExtended(toDate32('1970-01-08')) AS post_epoch
            )",
            R"(
┌─pre_epoch─┬─post_epoch─┐
│        -1 │          1 │
└───────────┴────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toRelativeWeekNum` wraps for pre-`1970-01-01` input; `toRelativeWeekNumExtended` stays monotonic",
            R"(
SELECT toRelativeWeekNum(toDateTime64('1960-01-01', 0, 'UTC'))        AS narrow_wraps,
       toRelativeWeekNumExtended(toDateTime64('1960-01-01', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│        65015 │        -521 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeWeekNumExtended>(documentation);
}

}


