#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;

REGISTER_FUNCTION(ToQuarter)
{
    FunctionDocumentation::Description description_to_quarter = R"(
Returns the quarter of the year (1-4) for a given `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax_to_quarter = "toQuarter(datetime)";
    FunctionDocumentation::Arguments arguments_to_quarter =
    {
        {"datetime", "Date or date with time to get the quarter of the year from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_quarter = {"Returns the quarter of the year for the given date/time", {"UInt8"}};
    FunctionDocumentation::Examples examples_to_quarter = {
        {"Usage example", R"(
SELECT toQuarter(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                            2 │
└──────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_quarter = {1, 1};
    FunctionDocumentation::Category category_to_quarter = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_quarter = {description_to_quarter, syntax_to_quarter, arguments_to_quarter, returned_value_to_quarter, examples_to_quarter, introduced_in_to_quarter, category_to_quarter};

    factory.registerFunction<FunctionToQuarter>(documentation_to_quarter);
    /// MySQL compatibility alias.
    factory.registerAlias("QUARTER", "toQuarter", FunctionFactory::Case::Insensitive);
}

}
