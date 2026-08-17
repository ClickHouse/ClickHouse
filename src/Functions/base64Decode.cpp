#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Decode
{
    static constexpr auto name = "base64Decode";
};

using Base64DecodeImpl = BaseXXDecode<Base64DecodeTraits<Base64Variant::Normal>, NameBase64Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase64Decode = FunctionBaseXXConversion<Base64DecodeImpl>;
}

REGISTER_FUNCTION(Base64Decode)
{
    FunctionDocumentation::Description description = R"(
Decodes a string from [Base64](https://en.wikipedia.org/wiki/Base64) representation, according to RFC 4648.
Throws an exception in case of error.

)";
    FunctionDocumentation::Syntax syntax = "base64Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode. If the string is not valid Base64-encoded, an exception is thrown.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the decoded string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base64Decode('Y2xpY2tob3VzZQ==')",
        R"(
┌─base64Decode('Y2xpY2tob3VzZQ==')─┐
│ clickhouse                       │
└──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase64Decode>(documentation);

    /// MySQL compatibility alias.
    factory.registerAlias("FROM_BASE64", "base64Decode", FunctionFactory::Case::Insensitive);
}

}

#endif
