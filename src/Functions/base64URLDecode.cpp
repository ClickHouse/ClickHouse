#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Decode
{
    static constexpr auto name = "base64URLDecode";
};

using Base64DecodeImpl = BaseXXDecode<Base64DecodeTraits<Base64Variant::URL>, NameBase64Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase64Decode = FunctionBaseXXConversion<Base64DecodeImpl>;
}

REGISTER_FUNCTION(Base64URLDecode)
{
    FunctionDocumentation::Description description = R"(Accepts a base64-encoded URL and decodes it from base64 with URL-specific modifications, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-5).)";
    FunctionDocumentation::Syntax syntax = "base64URLDecode(encodedURL)";
    FunctionDocumentation::Arguments arguments = {{"encodedURL", "String column or constant. If the string is not a valid Base64-encoded value, an exception is thrown."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the decoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t')", "https://clickhouse.com"}};
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;

    factory.registerFunction<FunctionBase64Decode>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}

}

#endif
