#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;

REGISTER_FUNCTION(ToDayOfYear)
{
    FunctionDocumentation::Description description = R"(
Returns the number of the day within the year (1-366) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax = "toDayOfYear(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A Date or DateTime value to get the day of year from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns the day of the year of the given Date or DateTime. [`UInt16`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toDayOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                            111 │
└────────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDayOfYear>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFYEAR", "toDayOfYear", FunctionFactory::Case::Insensitive);
}

}
