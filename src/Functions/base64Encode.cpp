#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base64Encode)
{
    FunctionDocumentation::Description description = R"(Encodes a String as base64, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-4). Alias: TO_BASE64.)";
    FunctionDocumentation::Syntax syntax = "base64Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {{"plaintext", "String column or constant."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the encoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64Encode('clickhouse')", "Y2xpY2tob3VzZQ=="}};
    FunctionDocumentation::Categories categories = {"String encoding"};

    factory.registerFunction<FunctionBase64Conversion<Base64Encode<Base64Variant::Normal>>>({description, syntax, arguments, returned_value, examples, categories});

    /// MySQL compatibility alias.
    factory.registerAlias("TO_BASE64", "base64Encode", FunctionFactory::Case::Insensitive);
}

}

#endif
