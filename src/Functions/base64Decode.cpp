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
    FunctionDocumentation::Description description = R"(Accepts a string and decodes it from base64, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-4). Throws an exception in case of an error. Alias: FROM_BASE64.)";
    FunctionDocumentation::Syntax syntax = "base64Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {{"encoded", "A string column. If the string is not a valid Base64-encoded value, an exception is thrown.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"A string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64Decode('Y2xpY2tob3VzZQ==')", "clickhouse"}};
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;

    factory.registerFunction<FunctionBase64Decode>({description, syntax, arguments, returned_value, examples, introduced_in, category});

    /// MySQL compatibility alias.
    factory.registerAlias("FROM_BASE64", "base64Decode", FunctionFactory::Case::Insensitive);
}

}

#endif
