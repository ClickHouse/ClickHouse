#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameTryBase32Decode
{
    static constexpr auto name = "tryBase32Decode";
};

using TryBase32DecodeImpl = BaseXXDecode<Base32DecodeTraits, NameTryBase32Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase32Decode = FunctionBaseXXConversion<TryBase32DecodeImpl>;
}

REGISTER_FUNCTION(TryBase32Decode)
{
    FunctionDocumentation::Description description = R"(
Accepts a string and decodes it using [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6) encoding scheme.
)";
    FunctionDocumentation::Syntax syntax = "tryBase32Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode. If the string is not valid Base32-encoded, returns an empty string in case of error.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT tryBase32Decode('IVXGG33EMVSA====');",
        R"(
┌─tryBase32Decode('IVXGG33EMVSA====')─┐
│ Encoded                             │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTryBase32Decode>(documentation);
}
}
