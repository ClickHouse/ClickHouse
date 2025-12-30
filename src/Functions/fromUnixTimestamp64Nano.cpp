#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Nano)
{
    /// fromUnixTimestamp64Nano documentation
    FunctionDocumentation::Description description_fromUnixTimestamp64Nano = R"(
Converts a Unix timestamp in nanoseconds to a [`DateTime64`](/sql-reference/data-types/datetime64) value with nanosecond precision.

The input value is treated as a Unix timestamp with nanosecond precision (number of nanoseconds since 1970-01-01 00:00:00 UTC).

:::note
Please note that the input value is treated as a UTC timestamp, not the timezone of the input value.
:::
    )";
    FunctionDocumentation::Syntax syntax_fromUnixTimestamp64Nano = "fromUnixTimestamp64Nano(value[, timezone])";
    FunctionDocumentation::Arguments arguments_fromUnixTimestamp64Nano = {
        {"value", "Unix timestamp in nanoseconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromUnixTimestamp64Nano = {"Returns a `DateTime64` value with nanosecond precision.", {"DateTime64(9)"}};
    FunctionDocumentation::Examples examples_fromUnixTimestamp64Nano = {
    {
        "Usage example",
        R"(
SELECT fromUnixTimestamp64Nano(1640995200123456789)
        )",
        R"(
┌─fromUnixTimestamp64Nano(1640995200123456789)─┐
│                2022-01-01 00:00:00.123456789 │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_fromUnixTimestamp64Nano = {20, 5};
    FunctionDocumentation::Category category_fromUnixTimestamp64Nano = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_fromUnixTimestamp64Nano = {description_fromUnixTimestamp64Nano, syntax_fromUnixTimestamp64Nano, arguments_fromUnixTimestamp64Nano, returned_value_fromUnixTimestamp64Nano, examples_fromUnixTimestamp64Nano, introduced_in_fromUnixTimestamp64Nano, category_fromUnixTimestamp64Nano};

    factory.registerFunction("fromUnixTimestamp64Nano",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(9, "fromUnixTimestamp64Nano", context); }, documentation_fromUnixTimestamp64Nano);
}

}
