#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64)
{
    /// fromUnixTimestamp64Second documentation
    FunctionDocumentation::Description description = R"(
Converts a Unix timestamp in seconds to a `DateTime64` value with second precision.

The input value is treated as a Unix timestamp with second precision (number of seconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax = "fromUnixTimestamp64Second(value[, timezone])";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Unix timestamp in seconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a `DateTime64` value with second precision.", {"DateTime64(0)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT fromUnixTimestamp64Second(1640995200)
        )",
        R"(
┌─fromUnixTimestamp64Second(1640995200)─┐
│                   2022-01-01 00:00:00 │
└───────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("fromUnixTimestamp64Second",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(0, "fromUnixTimestamp64Second", context); }, documentation);
}

}
