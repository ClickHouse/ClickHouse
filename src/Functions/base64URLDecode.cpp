#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Decode
{
    static constexpr auto name = "base64URLDecode";
};

using Base64DecodeImpl = BaseXXDecode<Base64DecodeTraits<Base64Variant::URL>, NameBase64Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase64Decode = FunctionBaseXXConversion<Base64DecodeImpl>;
}

REGISTER_FUNCTION(Base64URLDecode)
{
    FunctionDocumentation::Description description = R"(
Decodes a string from [Base64](https://en.wikipedia.org/wiki/Base64) representation using URL-safe alphabet, according to RFC 4648.
Throws an exception in case of error.
)";
    FunctionDocumentation::Syntax syntax = "base64URLDecode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to encode. If the string is not valid Base64-encoded, an exception is thrown.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base64URLDecode('aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ')",
        R"(
┌─base64URLDecode('aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ')─┐
│ https://clickhouse.com                            │
└───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase64Decode>(documentation);
}

}

#endif
