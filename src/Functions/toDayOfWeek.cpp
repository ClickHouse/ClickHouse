#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionCustomWeekToSomething.h>

namespace DB
{

using FunctionToDayOfWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToDayOfWeekImpl>;

REGISTER_FUNCTION(ToDayOfWeek)
{
    FunctionDocumentation::Description description = R"(
Returns the number of the day within the week of a `Date` or `DateTime` value.

The two-argument form of `toDayOfWeek()` enables you to specify whether the week starts on Monday or Sunday,
and whether the return value should be in the range from 0 to 6 or 1 to 7.

| Mode | First day of week | Range                                          |
|------|-------------------|------------------------------------------------|
| 0    | Monday            | 1-7: Monday = 1, Tuesday = 2, ..., Sunday = 7  |
| 1    | Monday            | 0-6: Monday = 0, Tuesday = 1, ..., Sunday = 6  |
| 2    | Sunday            | 0-6: Sunday = 0, Monday = 1, ..., Saturday = 6 |
| 3    | Sunday            | 1-7: Sunday = 1, Monday = 2, ..., Saturday = 7 |
        )";
    FunctionDocumentation::Syntax syntax = "toDayOfWeek(datetime[, mode[, timezone]])";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date or date with time to get the day of week from.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"mode", "Optional. Integer specifying the week mode (0-3). Defaults to 0 if omitted.", {"UInt8"}},
        {"timezone", "Optional. Timezone to use for the conversion.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the day of the week for the given `Date` or `DateTime`", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
-- The following date is April 21, 2023, which was a Friday:
SELECT
    toDayOfWeek(toDateTime('2023-04-21')),
    toDayOfWeek(toDateTime('2023-04-21'), 1)
            )",
        R"(
┌─toDayOfWeek(toDateTime('2023-04-21'))─┬─toDayOfWeek(toDateTime('2023-04-21'), 1)─┐
│                                     5 │                                        4 │
└───────────────────────────────────────┴──────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDayOfWeek>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFWEEK", "toDayOfWeek", FunctionFactory::Case::Insensitive);
}

}
