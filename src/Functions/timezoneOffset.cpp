#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctiontimezoneOffset = FunctionDateOrDateTimeToSomething<DataTypeInt32, TimezoneOffsetImpl>;

REGISTER_FUNCTION(timezoneOffset)
{
    FunctionDocumentation::Description description_timezone_offset = R"(
Returns the timezone offset in seconds from [UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time).
The function takes daylight saving time and historical timezone changes at the specified date and time into account.
    )";
    FunctionDocumentation::Syntax syntax_timezone_offset = "timeZoneOffset(datetime)";
    FunctionDocumentation::Arguments arguments_timezone_offset = {
        {"datetime", "`DateTime` value to get the timezone offset for.", {"DateTime", "DateTime64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_timezone_offset = {"Returns the offset from UTC in seconds", {"Int32"}};
    FunctionDocumentation::Examples examples_timezone_offset = {
        {"Usage example", R"(
SELECT toDateTime('2021-04-21 10:20:30', 'America/New_York') AS Time,
toTypeName(Time) AS Type,
timeZoneOffset(Time) AS Offset_in_seconds,
(Offset_in_seconds / 3600) AS Offset_in_hours;
        )",
        R"(
┌────────────────Time─┬─Type─────────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('America/New_York') │            -14400 │              -4 │
└─────────────────────┴──────────────────────────────┴───────────────────┴─────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_timezone_offset = {21, 6};
    FunctionDocumentation::Category category_timezone_offset = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_timezone_offset = {description_timezone_offset, syntax_timezone_offset, arguments_timezone_offset, returned_value_timezone_offset, examples_timezone_offset, introduced_in_timezone_offset, category_timezone_offset};

    factory.registerFunction<FunctiontimezoneOffset>(documentation_timezone_offset);
    factory.registerAlias("timeZoneOffset", "timezoneOffset");
}

}


