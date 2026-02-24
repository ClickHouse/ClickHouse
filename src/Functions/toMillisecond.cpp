#include <Common/FunctionDocumentation.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

using FunctionToMillisecond = FunctionDateOrDateTimeToSomething<DataTypeUInt16, ToMillisecondImpl>;

REGISTER_FUNCTION(ToMillisecond)
{
    FunctionDocumentation::Description description = R"(
Returns the millisecond component (0-999) of a `DateTime` or `DateTime64` value.
    )";
    FunctionDocumentation::Syntax syntax = "toMillisecond(datetime)";
    FunctionDocumentation::Arguments arguments =
    {
        {"datetime", "Date with time to get the millisecond from.", {"DateTime", "DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the millisecond in the minute (0 - 59) of `datetime`.", {"UInt16"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", R"(
SELECT toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3));
        )",
        R"(
┌──toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))─┐
│                                                        456 │
└────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToMillisecond>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("MILLISECOND", "toMillisecond", FunctionFactory::Case::Insensitive);
}

}
