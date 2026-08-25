#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase58Encode
{
    static constexpr auto name = "base58Encode";
};

using Base58EncodeImpl = BaseXXEncode<Base58EncodeTraits, NameBase58Encode>;
using FunctionBase58Encode = FunctionBaseXXConversion<Base58EncodeImpl>;
}
REGISTER_FUNCTION(Base58Encode)
{
    FunctionDocumentation::Description description = R"(
Encodes a string using [Base58](https://tools.ietf.org/id/draft-msporny-base58-01.html) encoding.
)";
    FunctionDocumentation::Syntax syntax = "base58Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {
        {"plaintext", "Plaintext to encode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the encoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base58Encode('ClickHouse');",
        R"(
┌─base58Encode('ClickHouse')─┐
│ 4nhk8K7GHXf6zx             │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase58Encode>(documentation);
}

}
