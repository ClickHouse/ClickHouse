#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

using FunctionToNanosecond = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToNanosecondImpl>;

REGISTER_FUNCTION(ToNanosecond)
{
    FunctionDocumentation::Description description = R"(
Returns the nanosecond component (0-999999999) of a `DateTime64` value.
    )";
    FunctionDocumentation::Syntax syntax = "toNanosecond(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to get the nanosecond from.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the nanosecond in the second (0 - 999999999) of `datetime`.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toNanosecond(toDateTime64('2023-04-21 10:20:30.123456789', 9));
        )",
        R"(
┌─toNanosecond(toDateTime64('2023-04-21 10:20:30.123456789', 9))─┐
│                                                      123456789 │
└────────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToNanosecond>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("NANOSECOND", "toNanosecond", FunctionFactory::Case::Insensitive);
}

}
