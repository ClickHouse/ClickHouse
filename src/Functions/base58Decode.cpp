#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase58Decode
{
    static constexpr auto name = "base58Decode";
};

using Base58DecodeImpl = BaseXXDecode<Base58DecodeTraits, NameBase58Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase58Decode = FunctionBaseXXConversion<Base58DecodeImpl>;
}

REGISTER_FUNCTION(Base58Decode)
{
    FunctionDocumentation::Description description = R"(
Decodes a [Base58](https://datatracker.ietf.org/doc/html/draft-msporny-base58-03#section-3) string.
If the string is not valid Base58-encoded, an exception is thrown.
An optional second argument `expected_size` can be provided to select an optimized fixed-size decoder.
Currently supported values are 32 and 64. For other values, the generic decoder is used.
When the optimized decoder is selected but the input cannot be decoded to exactly that many bytes,
the function throws an exception (or returns an empty string for `tryBase58Decode`).
)";
    FunctionDocumentation::Syntax syntax = "base58Decode(encoded[, expected_size])";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode.", {"String"}},
        {"expected_size", "Optional. Expected decoded size in bytes. When 32 or 64, an optimized decoder is used; for other values, the generic decoder is used.", {"UInt8, UInt16, UInt32, or UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base58Decode('JxF12TrwUP45BMd');",
        R"(
┌─base58Decode⋯rwUP45BMd')─┐
│ Hello World              │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase58Decode>(documentation);
}

}
