#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
REGISTER_FUNCTION(TryBase64Decode)
{
    FunctionDocumentation::Description description = R"(Decodes a String or FixedString from base64, like base64Decode but returns an empty string in case of an error.)";
    FunctionDocumentation::Syntax syntax = "tryBase64Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {{"encoded", "String column or constant. If the string is not a valid Base64-encoded value, returns an empty string."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the decoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"valid", "SELECT tryBase64Decode('Y2xpY2tob3VzZQ==')", "clickhouse"}, {"invalid", "SELECT tryBase64Decode('invalid')", ""}};
    FunctionDocumentation::Categories categories = {"String encoding"};

    factory.registerFunction<FunctionBase64Conversion<TryBase64Decode<Base64Variant::Normal>>>({description, syntax, arguments, returned_value, examples, categories});
}
}

#endif
