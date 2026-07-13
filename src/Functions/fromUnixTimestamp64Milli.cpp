#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Milli)
{
    /// fromUnixTimestamp64Milli documentation
    FunctionDocumentation::Description description = R"(
Converts a Unix timestamp in milliseconds to a `DateTime64` value with millisecond precision.

The input value is treated as a Unix timestamp with millisecond precision (number of milliseconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax = "fromUnixTimestamp64Milli(value[, timezone])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Unix timestamp in milliseconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A `DateTime64` value with millisecond precision.", {"DateTime64(3)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT fromUnixTimestamp64Milli(1640995200123)
        )",
        R"(
┌─fromUnixTimestamp64Milli(1640995200123)─┐
│                 2022-01-01 00:00:00.123 │
└─────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("fromUnixTimestamp64Milli",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(3, "fromUnixTimestamp64Milli", context); }, documentation);
}

}
