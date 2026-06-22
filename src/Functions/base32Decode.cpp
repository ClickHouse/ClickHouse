#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase32Decode
{
    static constexpr auto name = "base32Decode";
};

using Base32DecodeImpl = BaseXXDecode<Base32DecodeTraits, NameBase32Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase32Decode = FunctionBaseXXConversion<Base32DecodeImpl>;
}

REGISTER_FUNCTION(Base32Decode)
{
    FunctionDocumentation::Description description = R"(
Decodes a [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6) (RFC 4648) string.
If the string is not valid Base32-encoded, an exception is thrown.
)";
    FunctionDocumentation::Syntax syntax = "base32Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base32Decode('IVXGG33EMVSA====');",
        R"(
┌─base32Decode('IVXGG33EMVSA====')─┐
│ Encoded                          │
└──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase32Decode>(documentation);
}
}
