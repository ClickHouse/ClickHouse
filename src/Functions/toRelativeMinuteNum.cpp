#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeMinuteNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeMinuteNumImpl<ResultPrecision::Standard>>;
using FunctionToRelativeMinuteNumExtended = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToRelativeMinuteNumExtendedImpl>;

REGISTER_FUNCTION(ToRelativeMinuteNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of minutes elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in minutes between two dates or dates with time, e.g., `toRelativeMinuteNum(dt1) - toRelativeMinuteNum(dt2)`.
For an always-on, signed `Int64` variant that is monotonic on cross-epoch input and recommended in primary key and partition expressions, see [`toRelativeMinuteNumExtended`](#toRelativeMinuteNumExtended).
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeMinuteNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of minutes from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative minute numbers", R"(
SELECT toRelativeMinuteNum(toDateTime('2023-01-01 00:30:00')) - toRelativeMinuteNum(toDateTime('2023-01-01 00:00:00')) AS minutes_difference
        )",
        R"(
┌─minutes_difference─┐
│                 30 │
└────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeMinuteNum>(documentation);
}

REGISTER_FUNCTION(ToRelativeMinuteNumExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toRelativeMinuteNum`](#toRelativeMinuteNum), but returns a signed [`Int64`](/sql-reference/data-types/int-uint) instead of `UInt32`.

The wider, signed return type represents the full range of [`Date32`](/sql-reference/data-types/date32) and [`DateTime64`](/sql-reference/data-types/datetime64) arguments. The narrow `UInt32` result wraps to a large positive value for timestamps before `1970-01-01`.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toRelativeMinuteNum`](#toRelativeMinuteNum) function is monotonic only for arguments at or after `1970-01-01`.
    )";
    FunctionDocumentation::Syntax syntax = "toRelativeMinuteNumExtended(date)";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Number of minutes from a fixed reference point in the past, as a signed 64-bit integer.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Minutes across the epoch",
            R"(
SELECT toRelativeMinuteNumExtended(toDateTime64('1970-01-01 00:00:00', 0, 'UTC'))
     - toRelativeMinuteNumExtended(toDateTime64('1969-12-31 23:30:00', 0, 'UTC')) AS minutes_difference
            )",
            R"(
┌─minutes_difference─┐
│                 30 │
└────────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toRelativeMinuteNum` wraps for pre-`1970-01-01` input; `toRelativeMinuteNumExtended` stays monotonic",
            R"(
SELECT toRelativeMinuteNum(toDateTime64('1969-12-31 23:59:00', 0, 'UTC'))        AS narrow_wraps,
       toRelativeMinuteNumExtended(toDateTime64('1969-12-31 23:59:00', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│   4294967295 │          -1 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeMinuteNumExtended>(documentation);
}

}


