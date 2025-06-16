#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToISOYear = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToISOYearImpl>;

REGISTER_FUNCTION(ToISOYear)
{
    FunctionDocumentation::Description description_toISOYear = R"(
Converts a date or date with time to the ISO year as a `UInt16` number.
    )";
    FunctionDocumentation::Syntax syntax_toISOYear = R"(
toISOYear(datetime)
    )";
    FunctionDocumentation::Arguments arguments_toISOYear = {
        {"datetime", "The value with date or date with time. [`Date`](../data-types/date.md)/[`Date32`](../data-types/date32.md)/[`DateTime`](../data-types/datetime.md)/[`DateTime64`](../data-types/datetime64.md)"}
    };
    FunctionDocumentation::ReturnedValue returned_value_toISOYear = "Returns the input value converted to an ISO year number. [`UInt16`](../data-types/int-uint.md).";
    FunctionDocumentation::Examples examples_toISOYear = {
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
    FunctionDocumentation::IntroducedIn introduced_in_toISOYear = {18, 4};
    FunctionDocumentation::Category category_toISOYear = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toISOYear = {
        description_toISOYear,
        syntax_toISOYear,
        arguments_toISOYear,
        returned_value_toISOYear,
        examples_toISOYear,
        introduced_in_toISOYear,
        category_toISOYear
    };

    factory.registerFunction<FunctionToISOYear>(documentation_toISOYear);
}

}


