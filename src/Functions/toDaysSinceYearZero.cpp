#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

using FunctionToDaysSinceYearZero = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToDaysSinceYearZeroImpl>;

REGISTER_FUNCTION(ToDaysSinceYearZero)
{
    FunctionDocumentation::Description description = R"(
For a given date, returns the number of days which have passed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero) in the
[proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar).

The calculation is the same as in MySQL's [`TO_DAYS`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days) function.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toDaysSinceYearZero(date[, time_zone])
    )";
    FunctionDocumentation::Arguments arguments = {
        {"date", "The date or date with time for which to calculate the number of days since year zero from.", {"Date", "Date32", "DateTime", "DateTime64"}},
        {"time_zone", "Time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of days passed since date `0000-01-01`.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Calculate days since year zero", R"(
SELECT toDaysSinceYearZero(toDate('2023-09-08'))
        )",
        R"(
┌─toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                                     713569 │
└────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDaysSinceYearZero>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("TO_DAYS", FunctionToDaysSinceYearZero::name, FunctionFactory::Case::Insensitive);
}

}
