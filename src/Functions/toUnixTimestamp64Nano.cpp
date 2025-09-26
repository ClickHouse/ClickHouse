#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Nano)
{
    /// toUnixTimestamp64Nano documentation
    FunctionDocumentation::Description description_toUnixTimestamp64Nano = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/functions/type-conversion-functions#toint64) value with fixed nanosecond precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax_toUnixTimestamp64Nano = "toUnixTimestamp64Nano(value)";
    FunctionDocumentation::Arguments arguments_toUnixTimestamp64Nano = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUnixTimestamp64Nano = {"Returns a Unix timestamp in nanoseconds.", {"Int64"}};
    FunctionDocumentation::Examples examples_toUnixTimestamp64Nano = {
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
    FunctionDocumentation::IntroducedIn introduced_in_toUnixTimestamp64Nano = {20, 5};
    FunctionDocumentation::Category category_toUnixTimestamp64Nano = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_toUnixTimestamp64Nano = {description_toUnixTimestamp64Nano, syntax_toUnixTimestamp64Nano, arguments_toUnixTimestamp64Nano, returned_value_toUnixTimestamp64Nano, examples_toUnixTimestamp64Nano, introduced_in_toUnixTimestamp64Nano, category_toUnixTimestamp64Nano};

    factory.registerFunction("toUnixTimestamp64Nano",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(9, "toUnixTimestamp64Nano"); }, documentation_toUnixTimestamp64Nano);
}

}
