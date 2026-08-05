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
    FunctionDocumentation::Description description = R"(
Round the time to the start of a half-an-hour length interval.

:::note
Although this function can take values of the extended types `Date32` and `DateTime64` as an argument,
passing it a time outside the normal range (year 1970 to 2149 for `Date` / 2106 for `DateTime`) will produce wrong results.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
timeSlot(time[, time_zone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"time", "Time to round to the start of a half-an-hour length interval.", {"DateTime", "Date32", "DateTime64"}},
        {"time_zone", "Optional. A String type const value or an expression representing the time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the time rounded to the start of a half-an-hour length interval.", {"DateTime"}};
    FunctionDocumentation::Examples examples = {
        {"Round time to half-hour interval", R"(
SELECT timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))
        )",
        R"(
┌─timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))─┐
│                                2000-01-02 03:00:00 │
└────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSlot>(documentation);
}

}


