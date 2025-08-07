#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToDayOfMonthImpl>;

REGISTER_FUNCTION(ToDayOfMonth)
{
    FunctionDocumentation::Description description_to_day_of_month = R"(
Returns the day of the month (1-31) of a `Date` or `DateTime`.
        )";
    FunctionDocumentation::Syntax syntax_to_day_of_month = "toDayOfMonth(datetime)";
    FunctionDocumentation::Arguments arguments_to_day_of_month = {
        {"datetime", "A `Date` or `DateTime` value to get the day of month from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_day_of_month = "Returns the day of the month of the given date/time. [`UInt8`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_to_day_of_month = {
        {"Usage example", R"(
SELECT toDayOfMonth(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                              21 │
└─────────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_day_of_month = {1, 1};
    FunctionDocumentation::Category category_to_day_of_month = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_day_of_month = {description_to_day_of_month, syntax_to_day_of_month, arguments_to_day_of_month, returned_value_to_day_of_month, examples_to_day_of_month, introduced_in_to_day_of_month, category_to_day_of_month};

    factory.registerFunction<FunctionToDayOfMonth>(documentation_to_day_of_month);

    /// MySQL compatibility alias.
    factory.registerAlias("DAY", "toDayOfMonth", FunctionFactory::Case::Insensitive);
    factory.registerAlias("DAYOFMONTH", "toDayOfMonth", FunctionFactory::Case::Insensitive);
}

}
