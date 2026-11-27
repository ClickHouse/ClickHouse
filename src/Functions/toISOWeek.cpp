#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOWeek = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToISOWeekImpl>;

REGISTER_FUNCTION(ToISOWeek)
{
    FunctionDocumentation::Description description = R"(
Returns the ISO week number of a date or date with time.

This is a compatibility function that is equivalent to `toWeek(date, 3)`.
ISO weeks start on Monday and the first week of the year contains January 4th.
According to ISO 8601, week numbers are in the range from 1 to 53.

Note that dates near the beginning or end of a year may return a week number from the previous or next year. For example,
December 29, 2025 returns week 1 because it falls in the first week that contains January 4, 2026.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toISOWeek(datetime[, timezone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "Date or date with time to get the ISO week number from.", {"Date", "DateTime", "Date32", "DateTime64"}},
        {"timezone", "Optional. Time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the ISO week number according to ISO 8601 standard. Returns a number between 1 and 53.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Get ISO week numbers", R"(
SELECT toDate('2016-12-27') AS date, toISOWeek(date) AS isoWeek
        )",
        R"(
┌───────date─┬─isoWeek─┐
│ 2016-12-27 │      52 │
└────────────┴─────────┘
        )"},
        {"ISO week can belong to different year", R"(
SELECT toDate('2025-12-29') AS date, toISOWeek(date) AS isoWeek, toYear(date) AS year
        )",
        R"(
┌───────date─┬─isoWeek─┬─year─┐
│ 2025-12-29 │       1 │ 2025 │
└────────────┴─────────┴──────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToISOWeek>(documentation);
}

}


