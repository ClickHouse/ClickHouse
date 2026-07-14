#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Decode
{
static constexpr auto name = "tryBase64Decode";
};

using Base64DecodeImpl = BaseXXDecode<Base64DecodeTraits<Base64Variant::Normal>, NameBase64Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionBase64Decode = FunctionBaseXXConversion<Base64DecodeImpl>;
}
REGISTER_FUNCTION(TryBase64Decode)
{
    FunctionDocumentation::Description description = R"(
Like [`base64Decode`](#base64Decode), but returns an empty string in case of error.
)";
    FunctionDocumentation::Syntax syntax = "tryBase64Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode. If the string is not valid Base64-encoded, returns an empty string in case of error.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT tryBase64Decode('Y2xpY2tob3VzZQ==')",
        R"(
┌─tryBase64Decode('Y2xpY2tob3VzZQ==')─┐
│ clickhouse                          │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase64Decode>(documentation);
}
}

#endif
