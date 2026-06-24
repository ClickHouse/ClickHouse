#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToRelativeHourNum = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToRelativeHourNumImpl<ResultPrecision::Standard>>;
using FunctionToRelativeHourNumExtended = FunctionDateOrDateTimeToSomething<DataTypeInt64, ToRelativeHourNumExtendedImpl>;

REGISTER_FUNCTION(ToRelativeHourNum)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the number of hours elapsed since a certain fixed point in the past.
The exact point in time is an implementation detail, and therefore this function is not intended to be used standalone.
The main purpose of the function is to calculate the difference in hours between two dates or dates with time, e.g., `toRelativeHourNum(dt1) - toRelativeHourNum(dt2)`.
For an always-on, signed `Int64` variant that is monotonic on cross-epoch input and recommended in primary key and partition expressions, see [`toRelativeHourNumExtended`](#toRelativeHourNumExtended).
    )";
    FunctionDocumentation::Syntax syntax = R"(
toRelativeHourNum(date)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of hours from a fixed reference point in the past.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Get relative hour numbers", R"(
SELECT toRelativeHourNum(toDateTime('2023-01-01 12:00:00')) - toRelativeHourNum(toDateTime('2023-01-01 00:00:00')) AS hours_difference
        )",
        R"(
┌─hours_difference─┐
│               12 │
└──────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeHourNum>(documentation);
}

REGISTER_FUNCTION(ToRelativeHourNumExtended)
{
    FunctionDocumentation::Description description = R"(
Same as [`toRelativeHourNum`](#toRelativeHourNum), but returns a signed [`Int64`](/sql-reference/data-types/int-uint) and uses a single internal anchor for all timestamps, which keeps the result monotonic across the `1970-01-01` epoch.

Because the anchor differs from that of [`toRelativeHourNum`](#toRelativeHourNum), the absolute values of the two functions are not directly comparable. As with the other relative functions, only the difference between two results is meaningful.

It is therefore the recommended choice for primary key and partition key expressions. A monotonic function preserves the order of its input, so the primary index can still prune data when the function is applied to a key column. The [`toRelativeHourNum`](#toRelativeHourNum) function is monotonic only for arguments at or after `1970-01-01`.
    )";
    FunctionDocumentation::Syntax syntax = "toRelativeHourNumExtended(date)";
    FunctionDocumentation::Arguments arguments = {
        {"date", "Date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Number of hours from a fixed reference point in the past, as a signed 64-bit integer.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
        {
            "Hours across the epoch",
            R"(
SELECT toRelativeHourNumExtended(toDateTime64('1970-01-01 00:00:00', 0, 'UTC'))
     - toRelativeHourNumExtended(toDateTime64('1969-12-30 23:00:00', 0, 'UTC')) AS hours_difference
            )",
            R"(
┌─hours_difference─┐
│               25 │
└──────────────────┘
            )"
        },
        {
            "Side-by-side: narrow `toRelativeHourNum` returns wrong elapsed hours across the `1970-01-01` epoch (inconsistent narrow anchor); `toRelativeHourNumExtended` stays monotonic",
            R"(
WITH toDateTime64('1969-12-31 22:00:00', 0, 'UTC') AS t1,
     toDateTime64('1970-01-01 02:00:00', 0, 'UTC') AS t2
SELECT toRelativeHourNum(t2) - toRelativeHourNum(t1) AS narrow_diff_wrong,
       toRelativeHourNumExtended(t2) - toRelativeHourNumExtended(t1) AS extended_diff_ok
            )",
            R"(
┌─narrow_diff_wrong─┬─extended_diff_ok─┐
│               -20 │                4 │
└───────────────────┴──────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToRelativeHourNumExtended>(documentation);
}

}


