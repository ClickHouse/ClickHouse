#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToYearImpl>;

REGISTER_FUNCTION(ToYear)
{
    FunctionDocumentation::Description description_to_year = R"(
Returns the year component (AD) of a `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax_to_year = "toYear(datetime)";
    FunctionDocumentation::Arguments arguments_to_year = {
        {"datetime", "A `Date` or `DateTime` value to get the year from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_year = "Returns the year of the given Date or DateTime. [`UInt16`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples_to_year = {
        {"Usage example", R"(
    SELECT toYear(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
    ┌─toYear(toDateTime('2023-04-21 10:20:30'))─┐
    │                                     2023  │
    └───────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_year = {1, 1};
    FunctionDocumentation::Category category_to_year = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_year = {description_to_year, syntax_to_year, arguments_to_year, returned_value_to_year, examples_to_year, introduced_in_to_year, category_to_year};

    factory.registerFunction<FunctionToYear>(documentation_to_year);

    /// MySQL compatibility alias.
    factory.registerAlias("YEAR", "toYear", FunctionFactory::Case::Insensitive);
}

}
