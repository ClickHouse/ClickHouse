#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeDayNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeDayNumImpl<ResultPrecision::Standard>>;
using FunctionToRelativeDayNumExtended = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToRelativeDayNumExtendedImpl>;

REGISTER_FUNCTION(ToRelativeDayNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of days elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in days between two dates or dates with time, e.g., `toRelativeDayNum(dt1) - toRelativeDayNum(dt2)`.
For an always-on, signed `Int64` variant that is monotonic on cross-epoch input and recommended in primary key and partition expressions, see [`toRelativeDayNumExtended`](#toRelativeDayNumExtended).
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeDayNum(date)
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of days from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples =
    {
        {"Get relative day numbers", R"(
SELECT toRelativeDayNum(toDate('2023-04-01')) - toRelativeDayNum(toDate('2023-01-01'))
        )",
        R"(
┌─minus(toRela⋯3-01-01')))─┐
│                       90 │
└──────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeDayNum>(documentation);
}

REGISTER_FUNCTION(ToRelativeDayNumExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toRelativeDayNum`](#toRelativeDayNum), but returns a signed [`Int64`](/sql-reference/data-types/int-uint) instead of `UInt32`.

The wider, signed return type represents the full range of [`Date32`](/sql-reference/data-types/date32) and [`DateTime64`](/sql-reference/data-types/datetime64) arguments. The narrow `UInt32` result wraps to a large positive value for timestamps before `1970-01-01`, and again for dates after `2149-06-06`.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toRelativeDayNum`](#toRelativeDayNum) function is monotonic only for arguments between `1970-01-01` and `2149-06-06`.
    )";
    FunctionDocumentation::Syntax syntax = "toRelativeDayNumExtended(date)";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Number of days from a fixed reference point in the past, as a signed 64-bit integer.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Signed days across the epoch",
            R"(
SELECT toRelativeDayNumExtended(toDate32('1969-01-01')) AS pre_epoch,
       toRelativeDayNumExtended(toDate32('1970-01-01')) AS epoch
            )",
            R"(
┌─pre_epoch─┬─epoch─┐
│      -365 │     0 │
└───────────┴───────┘
            )"
        },
        {
            "Side-by-side: narrow `toRelativeDayNum` wraps for pre-`1970-01-01` input; `toRelativeDayNumExtended` stays monotonic",
            R"(
SELECT toRelativeDayNum(toDateTime64('1960-01-01', 0, 'UTC'))        AS narrow_wraps,
       toRelativeDayNumExtended(toDateTime64('1960-01-01', 0, 'UTC')) AS extended_ok
            )",
            R"(
┌─narrow_wraps─┬─extended_ok─┐
│        61883 │       -3653 │
└──────────────┴─────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeDayNumExtended>(documentation);
}

}


