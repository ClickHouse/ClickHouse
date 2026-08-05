#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>


namespace DB
{

using FunctionToTimeWithFixedDate = FunctionDateOrDateTimeToSomething<DataTypeDateTime, ToTimeWithFixedDateImpl>;

REGISTER_FUNCTION(ToTimeWithFixedDate)
{
    FunctionDocumentation::Description description = R"(
Extracts the time component of a date with time.
The returned result is an offset to a fixed point in time, currently `1970-01-02`,
but the exact point in time is an implementation detail which may change in future.

`toTimeWithFixedDate` should therefore not be used standalone.
The main purpose of the function is to calculate the time difference between two dates with time, e.g., `toTimeWithFixedDate(dt1) - toTimeWithFixedDate(dt2)`.

This is the legacy `toTime` function, renamed in v25.5 because the name `toTime` is now taken by
[`toTime`](/sql-reference/functions/type-conversion-functions#toTime), which converts values to the
[`Time`](/sql-reference/data-types/time) data type.
It remains reachable under its old name `toTime` when the setting
[`use_legacy_to_time`](/operations/settings/settings#use_legacy_to_time) is enabled (it defaults to `0` since v26.7, but defaulted to `1` from v25.6 to v26.6).

If the `datetime` argument has sub-second components, they are dropped in the returned `DateTime` value, which has second accuracy.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toTimeWithFixedDate(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date with time to convert to a time.", {"DateTime", "DateTime64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the time component of a date with time in the form of an offset to a fixed point in time (selected as 1970-01-02, currently).", {"DateTime"}};
    FunctionDocumentation::Examples examples = {
        {"Calculate the time difference between two dates", R"(
SELECT toTimeWithFixedDate('2025-06-15 12:00:00'::DateTime) - toTimeWithFixedDate('2024-05-10 11:00:00'::DateTime) AS result, toTypeName(result)
        )",
        R"(
┌─result─┬─toTypeName(result)─┐
│   3600 │ Int32              │
└────────┴────────────────────┘
        )"},
        {"Sub-second components are dropped", R"(
SELECT toTimeWithFixedDate(toDateTime64('1970-12-10 01:20:30.3000', 3)) AS result, toTypeName(result)
        )",
        R"(
┌──────────────result─┬─toTypeName(result)─┐
│ 1970-01-02 01:20:30 │ DateTime           │
└─────────────────────┴────────────────────┘
        )"},
        {"Same function called under its legacy name", R"(
SET use_legacy_to_time = 1;
SELECT toTime(toDateTime64('1970-12-10 01:20:30.3000', 3)) AS result, toTypeName(result)
        )",
        R"(
┌──────────────result─┬─toTypeName(result)─┐
│ 1970-01-02 01:20:30 │ DateTime           │
└─────────────────────┴────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToTimeWithFixedDate>(documentation);
}

}


