#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Encode
{
static constexpr auto name = "base64Encode";
};

using Base64EncodeImpl = BaseXXEncode<Base64EncodeTraits<Base64Variant::Normal>, NameBase64Encode>;
using FunctionBase64Encode = FunctionBaseXXConversion<Base64EncodeImpl>;
}

REGISTER_FUNCTION(Base64Encode)
{
    FunctionDocumentation::Description description = R"(
Encodes a string using [Base64](https://en.wikipedia.org/wiki/Base64) representation, according to RFC 4648.
)";
    FunctionDocumentation::Syntax syntax = "base64Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {
        {"plaintext", "Plaintext column or constant to decode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the encoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base64Encode('clickhouse')",
        R"(
┌─base64Encode('clickhouse')─┐
│ Y2xpY2tob3VzZQ==           │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase64Encode>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("TO_BASE64", "base64Encode", FunctionFactory::Case::Insensitive);
}

}

#endif
