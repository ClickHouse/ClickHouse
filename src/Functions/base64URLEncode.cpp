#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base64URLEncode)
{
    FunctionDocumentation::Description description = R"(Encodes an URL (String or FixedString) as base64 with URL-specific modifications, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-5).)";
    FunctionDocumentation::Syntax syntax = "base64URLEncode(url)";
    FunctionDocumentation::Arguments arguments = {{"url", "String column or constant."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the encoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64URLEncode('https://clickhouse.com')", "aHR0cHM6Ly9jbGlja2hvdXNlLmNvbQ"}};
    FunctionDocumentation::Categories categories = {"String encoding"};

    factory.registerFunction<FunctionBase64Conversion<Base64Encode<Base64Variant::URL>>>({description, syntax, arguments, returned_value, examples, categories});
}

}

#endif
