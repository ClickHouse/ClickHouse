#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToQuarter = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToQuarterImpl>;

REGISTER_FUNCTION(ToQuarter)
{
    FunctionDocumentation::Description description = R"(
Returns the quarter of the year (1-4) for a given `Date` or `DateTime` value.
    )";
    FunctionDocumentation::Syntax syntax = "toQuarter(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A `Date` or `DateTime` value to get the quarter of the year from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "The quarter of the year for the given date/time. [`UInt8`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toQuarter(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                            2 │
└──────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToQuarter>(documentation);
    /// MySQL compatibility alias.
    factory.registerAlias("QUARTER", "toQuarter", FunctionFactory::Case::Insensitive);
}

}
