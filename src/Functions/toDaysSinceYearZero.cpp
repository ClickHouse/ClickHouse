#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

using FunctionToDaysSinceYearZero = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToDaysSinceYearZeroImpl>;

REGISTER_FUNCTION(ToDaysSinceYearZero)
{
    FunctionDocumentation::Description description_toDaysSinceYearZero = R"(
For a given date, returns the number of days which have passed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero) in the
[proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar).

The calculation is the same as in MySQL's [`TO_DAYS`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days) function.
    )";
    FunctionDocumentation::Syntax syntax_toDaysSinceYearZero = R"(
toDaysSinceYearZero(date[, time_zone])
    )";
    FunctionDocumentation::Arguments arguments_toDaysSinceYearZero = {
        {"date", "The date or date with time for which to calculate the number of days since year zero from. [`Date`](../data-types/date.md) or [`Date32`](../data-types/date32.md) or [`DateTime`](../data-types/datetime.md) or [`DateTime64`](../data-types/datetime64.md)."},
        {"time_zone", "Time zone. [`String`](../data-types/string.md)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_toDaysSinceYearZero = "Returns the number of days passed since date `0000-01-01`. [`UInt32`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toDaysSinceYearZero = {
        {"Calculate days since year zero", R"(
SELECT toDaysSinceYearZero(toDate('2023-09-08'))
        )",
        R"(
┌─toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                                     713569 │
└────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toDaysSinceYearZero = {23, 9};
    FunctionDocumentation::Category category_toDaysSinceYearZero = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toDaysSinceYearZero = {
        description_toDaysSinceYearZero,
        syntax_toDaysSinceYearZero,
        arguments_toDaysSinceYearZero,
        returned_value_toDaysSinceYearZero,
        examples_toDaysSinceYearZero,
        introduced_in_toDaysSinceYearZero,
        category_toDaysSinceYearZero
    };

    factory.registerFunction<FunctionToDaysSinceYearZero>(documentation_toDaysSinceYearZero);

    /// MySQL compatibility alias.
    factory.registerAlias("TO_DAYS", FunctionToDaysSinceYearZero::name, FunctionFactory::Case::Insensitive);
}

}
