#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToMonth = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToMonthImpl>;

REGISTER_FUNCTION(ToMonth)
{
    FunctionDocumentation::Description description = R"(
Returns the month component (1-12) of a `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax = "toMonth(datetime)";
    FunctionDocumentation::Arguments arguments=
    {
        {"datetime", "Date or date with time to get the month from.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the month of the given date/time", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toMonth(toDateTime('2023-04-21 10:20:30'))
            )",
        R"(
┌─toMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          4 │
└────────────────────────────────────────────┘
            )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMonth>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("MONTH", "toMonth", FunctionFactory::Case::Insensitive);
}

}
