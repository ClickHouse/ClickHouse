#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeSecondNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeSecondNumImpl<ResultPrecision::Standard>>;
using FunctionToRelativeSecondNumExtended = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToRelativeSecondNumExtendedImpl>;

REGISTER_FUNCTION(ToRelativeSecondNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of seconds elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in seconds between two dates or dates with time, e.g., `toRelativeSecondNum(dt1) - toRelativeSecondNum(dt2)`.
For an always-on, signed `Int64` variant that is monotonic on cross-epoch input and recommended in primary key and partition expressions, see [`toRelativeSecondNumExtended`](#toRelativeSecondNumExtended).
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeSecondNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of seconds from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative second numbers", R"(
SELECT toRelativeSecondNum(toDateTime('2023-01-01 00:01:00')) - toRelativeSecondNum(toDateTime('2023-01-01 00:00:00')) AS seconds_difference
        )",
        R"(
┌─seconds_difference─┐
│                 60 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeSecondNum>(documentation);
}

REGISTER_FUNCTION(ToRelativeSecondNumExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toRelativeSecondNum`](#toRelativeSecondNum), but returns a signed [`Int64`](/sql-reference/data-types/int-uint) instead of `UInt32`.

The wider, signed return type represents the full range of [`Date32`](/sql-reference/data-types/date32) and [`DateTime64`](/sql-reference/data-types/datetime64) arguments. The narrow `UInt32` result wraps to a large positive value for timestamps before `1970-01-01`, and again after `2106-02-07 06:28:16 UTC`, beyond the `UInt32` second range.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toRelativeSecondNum`](#toRelativeSecondNum) function is monotonic only for arguments between `1970-01-01` and `2106-02-07 06:28:16`.
    )";
    FunctionDocumentation::Syntax syntax = "toRelativeSecondNumExtended(date)";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Number of seconds from a fixed reference point in the past, as a signed 64-bit integer.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Signed seconds across the epoch",
            R"(
SELECT toRelativeSecondNumExtended(toDateTime64('1969-12-31 23:59:59', 0, 'UTC')) AS just_before_epoch,
       toRelativeSecondNumExtended(toDateTime64('1970-01-01 00:00:00', 0, 'UTC')) AS epoch,
       toRelativeSecondNumExtended(toDateTime64('1970-01-01 00:00:01', 0, 'UTC')) AS just_after_epoch
            )",
            R"(
┌─just_before_epoch─┬─epoch─┬─just_after_epoch─┐
│                -1 │     0 │                1 │
└───────────────────┴───────┴──────────────────┘
            )"
        },
        {
            "Seconds past 2106 (the `UInt32` wrap point)",
            R"(
SELECT toRelativeSecondNumExtended(toDateTime64('2106-02-07 06:28:16', 0, 'UTC')) AS at_uint32_max,
       toRelativeSecondNumExtended(toDateTime64('2200-01-01 00:00:00', 0, 'UTC')) AS year_2200
            )",
            R"(
┌─at_uint32_max─┬──year_2200─┐
│    4294967296 │ 7258118400 │ -- 7.26 billion
└───────────────┴────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toRelativeSecondNum` wraps at `UInt32` max (`2106-02-07 06:28:16 UTC`); `toRelativeSecondNumExtended` stays monotonic",
            R"(
SELECT toRelativeSecondNum(toDateTime64('2106-02-07 06:28:16', 0, 'UTC'))        AS narrow_wraps,
       toRelativeSecondNumExtended(toDateTime64('2106-02-07 06:28:16', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│            0 │  4294967296 │ -- 4.29 billion
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeSecondNumExtended>(documentation);
}

}


