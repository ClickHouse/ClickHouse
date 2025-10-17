#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64Micro)
{
    /// fromUnixTimestamp64Micro documentation
    FunctionDocumentation::Description description_fromUnixTimestamp64Micro = R"(
Converts a Unix timestamp in microseconds to a `DateTime64` value with microsecond precision.

The input value is treated as a Unix timestamp with microsecond precision (number of microseconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax_fromUnixTimestamp64Micro = "fromUnixTimestamp64Micro(value[, timezone])";
    FunctionDocumentation::Arguments arguments_fromUnixTimestamp64Micro = {
        {"value", "Unix timestamp in microseconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromUnixTimestamp64Micro = {"Returns a `DateTime64` value with microsecond precision.", {"DateTime64(6)"}};
    FunctionDocumentation::Examples examples_fromUnixTimestamp64Micro = {
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
    FunctionDocumentation::IntroducedIn introduced_in_fromUnixTimestamp64Micro = {20, 5};
    FunctionDocumentation::Category category_fromUnixTimestamp64Micro = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_fromUnixTimestamp64Micro = {description_fromUnixTimestamp64Micro, syntax_fromUnixTimestamp64Micro, arguments_fromUnixTimestamp64Micro, returned_value_fromUnixTimestamp64Micro, examples_fromUnixTimestamp64Micro, introduced_in_fromUnixTimestamp64Micro, category_fromUnixTimestamp64Micro};

    factory.registerFunction("fromUnixTimestamp64Micro",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(6, "fromUnixTimestamp64Micro", context); }, documentation_fromUnixTimestamp64Micro);
}

}
