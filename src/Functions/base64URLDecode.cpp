#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base64URLDecode)
{
    FunctionDocumentation::Description description = R"(Accepts a base64-encoded URL and decodes it from base64 with URL-specific modifications, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-5).)";
    FunctionDocumentation::Syntax syntax = "base64URLDecode(encodedURL)";
    FunctionDocumentation::Arguments arguments = {{"encodedURL", "String column or constant. If the string is not a valid Base64-encoded value, an exception is thrown."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the decoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t')", "https://clickhouse.com"}};
    FunctionDocumentation::Categories categories = {"String encoding"};

    factory.registerFunction<FunctionBase64Conversion<Base64Decode<Base64Variant::URL>>>({description, syntax, arguments, returned_value, examples, categories});
}

}

#endif
