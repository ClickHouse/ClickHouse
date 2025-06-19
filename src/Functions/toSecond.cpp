#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToSecond = FunctionDateOrDateTimeToSomething<DataTypeUInt8, ToSecondImpl>;

REGISTER_FUNCTION(ToSecond)
{
    FunctionDocumentation::Description description = R"(
Returns the second component (0-59) of a `Date` or `DateTime` value.
        )";
    FunctionDocumentation::Syntax syntax = "toSecond(datetime)";
    FunctionDocumentation::Arguments arguments = {
        {"datetime", "A `Date` or `DateTime` value to get the second from. [`Date`](/sql-reference/data-types/date)/[`Date32`](/sql-reference/data-types/date32)/[`DateTime`](/sql-reference/data-types/datetime)/[`DateTime64`](/sql-reference/data-types/datetime64)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "Returns the second in the minute (0 - 59) of the given `Date` or `DateTime` value. [`UInt8`](/sql-reference/data-types/int-uint).";
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toSecond(toDateTime('2023-04-21 10:20:30'))
        )",
        R"(
┌─toSecond(toDateTime('2023-04-21 10:20:30'))─┐
│                                          30 │
└─────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToSecond>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("SECOND", "toSecond", FunctionFactory::Case::Insensitive);
}

}
