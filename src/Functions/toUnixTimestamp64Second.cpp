#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(ToUnixTimestamp64Second)
{
    /// toUnixTimestamp64Second documentation
    FunctionDocumentation::Description description = R"(
Converts a [`DateTime64`](/sql-reference/data-types/datetime64) to a [`Int64`](/sql-reference/data-types/int-uint) value with fixed second precision.
The input value is scaled up or down appropriately depending on its precision.

:::note
The output value is relative to UTC, not to the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax = "toUnixTimestamp64Second(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "DateTime64 value with any precision.", {"DateTime64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a Unix timestamp in seconds.", {"Int64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
WITH toDateTime64('2025-02-13 23:31:31.011', 3, 'UTC') AS dt64
SELECT toUnixTimestamp64Second(dt64);
        )",
        R"(
┌─toUnixTimestamp64Second(dt64)─┐
│                    1739489491 │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("toUnixTimestamp64Second",
        [](ContextPtr){ return std::make_shared<FunctionToUnixTimestamp64>(0, "toUnixTimestamp64Second"); }, documentation);
}

}
