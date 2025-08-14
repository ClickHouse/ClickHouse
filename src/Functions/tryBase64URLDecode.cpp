#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Decode
{
static constexpr auto name = "tryBase64URLDecode";
};

using Base64DecodeImpl = BaseXXDecode<Base64DecodeTraits<Base64Variant::URL>, NameBase64Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionBase64Decode = FunctionBaseXXConversion<Base64DecodeImpl>;
}
REGISTER_FUNCTION(TryBase64URLDecode)
{
    FunctionDocumentation::Description description = R"(Decodes an URL from base64, like base64URLDecode but returns an empty string in case of an error.)";
    FunctionDocumentation::Syntax syntax = "tryBase64URLDecode(encodedUrl)";
    FunctionDocumentation::Arguments arguments = {{"encodedURL", "String column or constant. If the string is not a valid Base64-encoded value with URL-specific modifications, returns an empty string."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the decoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"valid", "SELECT tryBase64URLDecode('aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ')", "https://clickhouse.com"}, {"invalid", "SELECT tryBase64UrlDecode('aHR0cHM6Ly9jbGlja')", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;

    factory.registerFunction<FunctionBase64Decode>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}
}

#endif
