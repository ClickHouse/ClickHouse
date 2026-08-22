#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctiontimezoneOffset = FunctionDateOrDateTimeToSomething<DataTypeInt32, TimezoneOffsetImpl>;

REGISTER_FUNCTION(timezoneOffset)
{
    FunctionDocumentation::Description description = R"(
Returns the timezone offset in seconds from [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
The function takes daylight saving time and historical timezone changes at the specified date and time into account.
    )";
    FunctionDocumentation::Syntax syntax = "timezoneOffset(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "`DateTime` value to get the timezone offset for.", {"DateTime", "DateTime64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the offset from UTC in seconds", {"Int32"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDateTime('2021-04-21 10:20:30', 'America/New_York') AS Time,
toTypeName(Time) AS Type,
timezoneOffset(Time) AS Offset_in_seconds,
(Offset_in_seconds / 3600) AS Offset_in_hours;
        )",
        R"(
┌────────────────Time─┬─Type─────────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('America/New_York') │            -14400 │              -4 │
└─────────────────────┴──────────────────────────────┴───────────────────┴─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctiontimezoneOffset>(documentation);
    factory.registerAlias("timeZoneOffset", "timezoneOffset");
}

}


