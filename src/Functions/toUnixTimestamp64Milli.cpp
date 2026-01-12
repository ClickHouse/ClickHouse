#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Milli)
{
    /// toUnixTimestamp64Milli documentation
    FunctionDocumentation::Description description_toUnixTimestamp64Milli = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/data-types/int-uint) value with fixed millisecond precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax_toUnixTimestamp64Milli = "toUnixTimestamp64Milli(value)";
    FunctionDocumentation::Arguments arguments_toUnixTimestamp64Milli = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUnixTimestamp64Milli = {"Returns a Unix timestamp in milliseconds.", {"Int64"}};
    FunctionDocumentation::Examples examples_toUnixTimestamp64Milli = {
    {
        "Usage example",
        R"(
WITH toDateTime64('2025-02-13 23:31:31.011', 3, 'UTC') AS dt64
SELECT toUnixTimestamp64Milli(dt64);
        )",
        R"(
┌─toUnixTimestamp64Milli(dt64)─┐
│                1739489491011 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUnixTimestamp64Milli = {20, 5};
    FunctionDocumentation::Category category_toUnixTimestamp64Milli = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUnixTimestamp64Milli = {description_toUnixTimestamp64Milli, syntax_toUnixTimestamp64Milli, arguments_toUnixTimestamp64Milli, returned_value_toUnixTimestamp64Milli, examples_toUnixTimestamp64Milli, introduced_in_toUnixTimestamp64Milli, category_toUnixTimestamp64Milli};

    factory.registerFunction("toUnixTimestamp64Milli",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(3, "toUnixTimestamp64Milli"); }, documentation_toUnixTimestamp64Milli);
}

}
