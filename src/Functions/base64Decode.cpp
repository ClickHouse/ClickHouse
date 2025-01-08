#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base64Decode)
{
    FunctionDocumentation::Description description = R"(Accepts a String and decodes it from base64, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-4). Throws an exception in case of an error. Alias: FROM_BASE64.)";
    FunctionDocumentation::Syntax syntax = "base64Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {{"encoded", "String column or constant. If the string is not a valid Base64-encoded value, an exception is thrown."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the decoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64Decode('Y2xpY2tob3VzZQ==')", "clickhouse"}};
    FunctionDocumentation::Categories categories = {"String encoding"};

    factory.registerFunction<FunctionBase64Conversion<Base64Decode<Base64Variant::Normal>>>({description, syntax, arguments, returned_value, examples, categories});

    /// MySQL compatibility alias.
    factory.registerAlias("FROM_BASE64", "base64Decode", FunctionFactory::Case::Insensitive);
}

}

#endif
