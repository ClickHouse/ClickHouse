#include <Functions/FunctionBase64Conversion.h>

#if USE_BASE64
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase64Encode
{
static constexpr auto name = "base64Encode";
};

using Base64EncodeImpl = BaseXXEncode<Base64EncodeTraits<Base64Variant::Normal>, NameBase64Encode>;
using FunctionBase64Encode = FunctionBaseXXConversion<Base64EncodeImpl>;
}

REGISTER_FUNCTION(Base64Encode)
{
    FunctionDocumentation::Description description = R"(Encodes a String as base64, according to RFC 4648 (https://datatracker.ietf.org/doc/html/rfc4648#section-4). Alias: TO_BASE64.)";
    FunctionDocumentation::Syntax syntax = "base64Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {{"plaintext", "String column or constant."}};
    FunctionDocumentation::ReturnedValue returned_value = "A string containing the encoded value of the argument.";
    FunctionDocumentation::Examples examples = {{"Example", "SELECT base64Encode('clickhouse')", "Y2xpY2tob3VzZQ=="}};
    FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;

    factory.registerFunction<FunctionBase64Encode>({description, syntax, arguments, returned_value, examples, introduced_in, category});

    /// MySQL compatibility alias.
    factory.registerAlias("TO_BASE64", "base64Encode", FunctionFactory::Case::Insensitive);
}

}

#endif
