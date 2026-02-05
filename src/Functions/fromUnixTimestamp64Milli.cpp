#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Milli)
{
    /// fromUnixTimestamp64Milli documentation
    FunctionDocumentation::Description description_fromUnixTimestamp64Milli = R"(
Converts a Unix timestamp in milliseconds to a `DateTime64` value with millisecond precision.

The input value is treated as a Unix timestamp with millisecond precision (number of milliseconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax_fromUnixTimestamp64Milli = "fromUnixTimestamp64Milli(value[, timezone])";
    FunctionDocumentation::Arguments arguments_fromUnixTimestamp64Milli = {
        {"value", "Unix timestamp in milliseconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromUnixTimestamp64Milli = {"A `DateTime64` value with millisecond precision.", {"DateTime64(3)"}};
    FunctionDocumentation::Examples examples_fromUnixTimestamp64Milli = {
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
    FunctionDocumentation::IntroducedIn introduced_in_fromUnixTimestamp64Milli = {20, 5};
    FunctionDocumentation::Category category_fromUnixTimestamp64Milli = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_fromUnixTimestamp64Milli = {description_fromUnixTimestamp64Milli, syntax_fromUnixTimestamp64Milli, arguments_fromUnixTimestamp64Milli, returned_value_fromUnixTimestamp64Milli, examples_fromUnixTimestamp64Milli, introduced_in_fromUnixTimestamp64Milli, category_fromUnixTimestamp64Milli};

    factory.registerFunction("fromUnixTimestamp64Milli",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(3, "fromUnixTimestamp64Milli", context); }, documentation_fromUnixTimestamp64Milli);
}

}
