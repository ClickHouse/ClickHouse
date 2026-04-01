#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Micro)
{
    /// fromUnixTimestamp64Micro documentation
    FunctionDocumentation::Description description = R"(
Converts a Unix timestamp in microseconds to a `DateTime64` value with microsecond precision.

The input value is treated as a Unix timestamp with microsecond precision (number of microseconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax = "fromUnixTimestamp64Micro(value[, timezone])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Unix timestamp in microseconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a `DateTime64` value with microsecond precision.", {"DateTime64(6)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT fromUnixTimestamp64Micro(1640995200123456)
        )",
        R"(
┌─fromUnixTimestamp64Micro(1640995200123456)─┐
│                 2022-01-01 00:00:00.123456 │
└────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("fromUnixTimestamp64Micro",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(6, "fromUnixTimestamp64Micro", context); }, documentation);
}

}
