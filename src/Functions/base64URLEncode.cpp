#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Encode
{
static constexpr auto name = "base64URLEncode";
};

using Base64EncodeImpl = BaseXXEncode<Base64EncodeTraits<Base64Variant::URL>, NameBase64Encode>;
using FunctionBase64Encode = FunctionBaseXXConversion<Base64EncodeImpl>;
}

REGISTER_FUNCTION(Base64URLEncode)
{
    FunctionDocumentation::Description description = R"(Encodes an URL (String or FixedString) as base64 with URL-specific modifications, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-5).)";
    FunctionDocumentation::Syntax syntax = "base64URLEncode(url)";
    FunctionDocumentation::Arguments arguments = {{"url", "String column or constant."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the encoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64URLEncode('https://clickhouse.com')", "aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ"}};
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;

    factory.registerFunction<FunctionBase64Encode>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}

}

#endif
