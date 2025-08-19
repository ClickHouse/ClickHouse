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
Decodes a string from [Base58](https://tools.ietf.org/id/draft-msporny-base58-01.html) encoding.
An exception is thrown in case of an error.
)";
    FunctionDocumentation::Syntax syntax = "base58Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode. If the string is not valid Base58-encoded, an exception is thrown.", {"String"}}
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase58Decode>(documentation);
}

}
