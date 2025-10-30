#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionCustomWeekToSomething.h>

namespace DB
{

using FunctionToDayOfWeek = FunctionCustomWeekToSomething<DataTypeUInt8, ToDayOfWeekImpl>;

REGISTER_FUNCTION(ToDayOfWeek)
{
    FunctionDocumentation::Description description_to_day_of_week = R"(
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
    FunctionDocumentation::Syntax syntax_to_day_of_week = "toDayOfWeek(datetime[, mode[, timezone]])";
    FunctionDocumentation::Arguments arguments_to_day_of_week = {
        {"datetime", "A Date or DateTime value to get the day of week from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."},
        {"mode", "Optional. Integer specifying the week mode (0-3). Defaults to 0 if omitted."},
        {"timezone", "Optional. String specifying the timezone."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_day_of_week = "Returns the day of the week for the given `Date` or `DateTime`. [`UInt8`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_to_day_of_week = {
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
    FunctionDocumentation::IntroducedIn introduced_in_to_day_of_week = {1, 1};
    FunctionDocumentation::Category category_to_day_of_week = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_day_of_week = {description_to_day_of_week, syntax_to_day_of_week, arguments_to_day_of_week, returned_value_to_day_of_week, examples_to_day_of_week, introduced_in_to_day_of_week, category_to_day_of_week};

    factory.registerFunction<FunctionToDayOfWeek>(documentation_to_day_of_week);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFWEEK", "toDayOfWeek", FunctionFactory::Case::Insensitive);
}

}
