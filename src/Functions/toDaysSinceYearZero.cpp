#include <DataTypes/DataTypesNumber.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

using FunctionToDaysSinceYearZero = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToDaysSinceYearZeroImpl>;

REGISTER_FUNCTION(ToDaysSinceYearZero)
{
    factory.registerFunction<FunctionToDaysSinceYearZero>(FunctionDocumentation{
        .description = R"(
Returns for a given date or date with time, the number of days passed since 1 January 0000 in the proleptic Gregorian calendar defined by ISO 8601.
The calculation is the same as in MySQL's TO_DAYS() function.
)",
        .syntax="toDaysSinceYearZero(date[, time_zone])",
        .arguments={
            {"date", "The date to calculate the number of days passed since year zero from. [Date](../data-types/date.md), [Date32](../data-types/date32.md), [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md)."},
            {"time_zone", "A String type const value or an expression represent the time zone. [String types](../data-types/string.md)"}
        },
        .returned_value="The number of days passed since date 0000-01-01. [UInt32](../data-types/int-uint.md).",
        .examples{{"typical", "SELECT toDaysSinceYearZero(toDate('2023-09-08'))", "713569"}},
        .category=FunctionDocumentation::Category::DateAndTime,
        .related={"[fromDaysSinceYearZero](#fromdayssinceyearzero)"}     
        )"
    });

    /// MySQL compatibility alias.
    factory.registerAlias("TO_DAYS", FunctionToDaysSinceYearZero::name, FunctionFactory::Case::Insensitive);
}

}
