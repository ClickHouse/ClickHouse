#include <Functions/FunctionUnixTimestamp64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(FromUnixTimestamp64)
{
    /// fromUnixTimestamp64Second documentation
    FunctionDocumentation::Description description_fromUnixTimestamp64Second = R"(
Converts a Unix timestamp in seconds to a `DateTime64` value with second precision.

The input value is treated as a Unix timestamp with second precision (number of seconds since 1970-01-01 00:00:00 UTC).
    )";
    FunctionDocumentation::Syntax syntax_fromUnixTimestamp64Second = "fromUnixTimestamp64Second(value[, timezone])";
    FunctionDocumentation::Arguments arguments_fromUnixTimestamp64Second = {
        {"value", "Unix timestamp in seconds.", {"Int64"}},
        {"timezone", "Optional. Timezone for the returned value.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromUnixTimestamp64Second = {"Returns a `DateTime64` value with second precision.", {"DateTime64(0)"}};
    FunctionDocumentation::Examples examples_fromUnixTimestamp64Second = {
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
    FunctionDocumentation::IntroducedIn introduced_in_fromUnixTimestamp64Second = {24, 12};
    FunctionDocumentation::Category category_fromUnixTimestamp64Second = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation_fromUnixTimestamp64Second = {description_fromUnixTimestamp64Second, syntax_fromUnixTimestamp64Second, arguments_fromUnixTimestamp64Second, returned_value_fromUnixTimestamp64Second, examples_fromUnixTimestamp64Second, introduced_in_fromUnixTimestamp64Second, category_fromUnixTimestamp64Second};

    factory.registerFunction("fromUnixTimestamp64Second",
        [](ContextPtr context){ return std::make_shared<FunctionFromUnixTimestamp64>(0, "fromUnixTimestamp64Second", context); }, documentation_fromUnixTimestamp64Second);
}

}
