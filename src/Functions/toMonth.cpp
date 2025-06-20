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
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A Date or DateTime value to get the month from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns the month of the given date/time. [`UInt8`](/sql-reference/data-types/int-uint).";
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
