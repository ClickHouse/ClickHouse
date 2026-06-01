#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

using FunctionToMicrosecond = FunctionDateOrDateTimeToSomething<DataTypeUInt32, ToMicrosecondImpl>;

REGISTER_FUNCTION(ToMicrosecond)
{
    FunctionDocumentation::Description description = R"(
Returns the microsecond component (0-999999) of a `DateTime64` or `Time64` value.
    )";
    FunctionDocumentation::Syntax syntax = "toMicrosecond(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to get the microsecond from.", {"DateTime64", "Time64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the microsecond in the second (0 - 999999) of `datetime`.", {"UInt32"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toMicrosecond(toDateTime64('2023-04-21 10:20:30.456789', 6));
        )",
        R"(
┌─toMicrosecond(toDateTime64('2023-04-21 10:20:30.456789', 6))─┐
│                                                       456789 │
└──────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMicrosecond>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("MICROSECOND", "toMicrosecond", FunctionFactory::Case::Insensitive);
}

}
