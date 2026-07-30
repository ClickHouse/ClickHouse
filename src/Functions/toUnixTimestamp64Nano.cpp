#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Nano)
{
    /// toUnixTimestamp64Nano documentation
    FunctionDocumentation::Description description = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/functions/type-conversion-functions#toInt64) value with fixed nanosecond precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax = "toUnixTimestamp64Nano(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a Unix timestamp in nanoseconds.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
WITH toDateTime64('2025-02-13 23:31:31.011123456', 9, 'UTC') AS dt64
SELECT toUnixTimestamp64Nano(dt64);
        )",
        R"(
┌─toUnixTimestamp64Nano(dt64)────┐
│            1739489491011123456 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("toUnixTimestamp64Nano",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(9, "toUnixTimestamp64Nano"); }, documentation);
}

}
