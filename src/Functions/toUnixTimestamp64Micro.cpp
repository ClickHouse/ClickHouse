#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Micro)
{
    /// toUnixTimestamp64Micro documentation
    FunctionDocumentation::Description description = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/data-types/int-uint) value with fixed microsecond precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax = "toUnixTimestamp64Micro(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a Unix timestamp in microseconds.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
WITH toDateTime64('2025-02-13 23:31:31.011123', 6, 'UTC') AS dt64
SELECT toUnixTimestamp64Micro(dt64);
        )",
        R"(
┌─toUnixTimestamp64Micro(dt64)─┐
│               1739489491011123 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("toUnixTimestamp64Micro",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(6, "toUnixTimestamp64Micro"); }, documentation);
}

}
