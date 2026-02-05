#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Micro)
{
    /// toUnixTimestamp64Micro documentation
    FunctionDocumentation::Description description_toUnixTimestamp64Micro = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/data-types/int-uint) value with fixed microsecond precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax_toUnixTimestamp64Micro = "toUnixTimestamp64Micro(value)";
    FunctionDocumentation::Arguments arguments_toUnixTimestamp64Micro = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUnixTimestamp64Micro = {"Returns a Unix timestamp in microseconds.", {"Int64"}};
    FunctionDocumentation::Examples examples_toUnixTimestamp64Micro = {
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
    FunctionDocumentation::IntroducedIn introduced_in_toUnixTimestamp64Micro = {20, 5};
    FunctionDocumentation::Category category_toUnixTimestamp64Micro = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUnixTimestamp64Micro = {description_toUnixTimestamp64Micro, syntax_toUnixTimestamp64Micro, arguments_toUnixTimestamp64Micro, returned_value_toUnixTimestamp64Micro, examples_toUnixTimestamp64Micro, introduced_in_toUnixTimestamp64Micro, category_toUnixTimestamp64Micro};

    factory.registerFunction("toUnixTimestamp64Micro",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(6, "toUnixTimestamp64Micro"); }, documentation_toUnixTimestamp64Micro);
}

}
