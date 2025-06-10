#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToDateTimeOrDateTime64.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionTimeSlot = FunctionDateOrDateTimeToDateTimeOrDateTime64<TimeSlotImpl>;

REGISTER_FUNCTION(TimeSlot)
{
    FunctionDocumentation::Description description_timeSlot = R"(
Round the time to the start of a half-an-hour length interval.

:::note
Although this function can take values of the extended types `Date32` and `DateTime64` as an argument,
passing it a time outside the normal range (year 1970 to 2149 for `Date` / 2106 for `DateTime`) will produce wrong results.
:::
    )";
    FunctionDocumentation::Syntax syntax_timeSlot = R"(
timeSlot(time[, time_zone])
    )";
    FunctionDocumentation::Arguments arguments_timeSlot = {
        {"time", "Time to round to the start of a half-an-hour length interval. [`DateTime`](../data-types/datetime.md)/[`Date32`](../data-types/date32.md)/[`DateTime64`](../data-types/datetime64.md)."},
        {"time_zone", "Optional. A String type const value or an expression representing the time zone. [`String`](../data-types/string.md)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_timeSlot = "Returns the time rounded to the start of a half-an-hour length interval. [`DateTime`](../data-types/datetime.md).";
    FunctionDocumentation::Examples examples_timeSlot = {
        {"Round time to half-hour interval", R"(
SELECT timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))
        )",
        R"(
┌─timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))─┐
│                                2000-01-02 03:00:00 │
└────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_timeSlot = {1, 1};
    FunctionDocumentation::Category category_timeSlot = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_timeSlot = {
        description_timeSlot,
        syntax_timeSlot,
        arguments_timeSlot,
        returned_value_timeSlot,
        examples_timeSlot,
        introduced_in_timeSlot,
        category_timeSlot
    };

    factory.registerFunction<FunctionTimeSlot>(documentation_timeSlot);
}

}


