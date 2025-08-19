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
Decodes a string from [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6), according to RFC 4648.
Throws an exception in case of error.
)";
    FunctionDocumentation::Syntax syntax = "base32Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "[String](../data-types/string.md) column or constant. If the string is not valid Base32-encoded, an exception is thrown.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Basic usage", "SELECT base32Decode('IVXGG33EMVSA====');", "┌─base32Decode('IVXGG33EMVSA====')─┐\n│ Encoded                          │\n└──────────────────────────────────┘"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase32Decode>(documentation);
}
}
