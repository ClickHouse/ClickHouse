#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToDayOfYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToDayOfYearImpl>;

REGISTER_FUNCTION(ToDayOfYear)
{
    FunctionDocumentation::Description description_to_day_of_year = R"(
Returns the number of the day within the year (1-366) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax_to_day_of_year = "toDayOfYear(datetime)";
    FunctionDocumentation::Arguments arguments_to_day_of_year =
    {
        {"datetime", "Date or date with time to get the day of year from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_day_of_year = {"Returns the day of the year of the given Date or DateTime", {"UInt16"}};
    FunctionDocumentation::Examples examples_to_day_of_year = {
        {"Usage example", R"(
SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toDayOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                            111 │
└────────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_day_of_year = {18, 4};
    FunctionDocumentation::Category category_to_day_of_year = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_day_of_year = {description_to_day_of_year, syntax_to_day_of_year, arguments_to_day_of_year, returned_value_to_day_of_year, examples_to_day_of_year, introduced_in_to_day_of_year, category_to_day_of_year};

    factory.registerFunction<FunctionToDayOfYear>(documentation_to_day_of_year);

    /// MySQL compatibility alias.
    factory.registerAlias("DAYOFYEAR", "toDayOfYear", FunctionFactory::Case::Insensitive);
}

}
