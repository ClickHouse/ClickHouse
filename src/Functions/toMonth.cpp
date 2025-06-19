#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;

REGISTER_FUNCTION(ToMonth)
{
    FunctionDocumentation::Description description_to_month = R"(
Returns the month component (1-12) of a `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax_to_month = "toMonth(datetime)";
    FunctionDocumentation::Arguments arguments_to_month =
    {
        {"datetime", "Date or date with time to get the month from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_to_month = {"Returns the month of the given date/time", {"UInt8"}};
    FunctionDocumentation::Examples examples_to_month = {
        {"Usage example", R"(
SELECT toMonth(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          4 │
└────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_to_month = {1, 1};
    FunctionDocumentation::Category category_to_month = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_to_month = {description_to_month, syntax_to_month, arguments_to_month, returned_value_to_month, examples_to_month, introduced_in_to_month, category_to_month};

    factory.registerFunction<FunctionToMonth>(documentation_to_month);

    /// MySQL compatibility alias.
    factory.registerAlias("MONTH", "toMonth", FunctionFactory::Case::Insensitive);
}

}
