#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Encode
{
static constexpr auto name = "base64URLEncode";
};

using Base64EncodeImpl = BaseXXEncode<Base64EncodeTraits<Base64Variant::URL>, NameBase64Encode>;
using FunctionBase64Encode = FunctionBaseXXConversion<Base64EncodeImpl>;
}

REGISTER_FUNCTION(Base64URLEncode)
{
    FunctionDocumentation::Description description = R"(
Encodes a string using [Base64](https://datatracker.ietf.org/doc/html/rfc4648#section-4) (RFC 4648) representation using URL-safe alphabet.
)";
    FunctionDocumentation::Syntax syntax = "base64URLEncode(plaintext)";
    FunctionDocumentation::Arguments arguments = {
        {"plaintext", "Plaintext column or constant to encode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the encoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base64URLEncode('https://clickhouse.com')",
        R"(
┌─base64URLEncode('https://clickhouse.com')─┐
│ aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ            │
└───────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase64Encode>(documentation);
}

}

#endif
