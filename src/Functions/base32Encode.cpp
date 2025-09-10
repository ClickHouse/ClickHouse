#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase32Encode
{
    static constexpr auto name = "base32Encode";
};

using Base32EncodeImpl = BaseXXEncode<Base32EncodeTraits, NameBase32Encode>;
using FunctionBase32Encode = FunctionBaseXXConversion<Base32EncodeImpl>;
}

REGISTER_FUNCTION(Base32Encode)
{
    FunctionDocumentation::Description description = R"(
Encodes a string using [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6).
)";
    FunctionDocumentation::Syntax syntax = "base32Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {
        {"plaintext", "Plaintext to encode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the encoded value of the argument.", {"String", "FixedString"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base32Encode('Encoded')",
        R"(
┌─base32Encode('Encoded')─┐
│ IVXGG33EMVSA====        │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase32Encode>(documentation);
}
}
