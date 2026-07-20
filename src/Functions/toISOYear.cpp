#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToISOYearImpl>;

REGISTER_FUNCTION(ToISOYear)
{
    FunctionDocumentation::Description description = R"(
Converts a date or date with time to the ISO year number.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toISOYear(datetime)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "The value with date or date with time.", {"Date", "Date32", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the input value converted to an ISO year number.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
        {"Get ISO year from date values", R"(
SELECT
toISOYear(toDate('2024/10/02')) as year1,
toISOYear(toDateTime('2024-10-02 01:30:00')) as year2
        )",
        R"(
┌─week1─┬─week2─┐
│    40 │    40 │
└───────┴───────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToISOYear>(documentation);
}

}


