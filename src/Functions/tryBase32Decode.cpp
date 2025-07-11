#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameTryBase32Decode
{
    static constexpr auto name = "tryBase32Decode";
};

using TryBase32DecodeImpl = BaseXXDecode<Base32DecodeTraits, NameTryBase32Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase32Decode = FunctionBaseXXConversion<TryBase32DecodeImpl>;
}

REGISTER_FUNCTION(TryBase32Decode)
{
    factory.registerFunction<FunctionTryBase32Decode>(FunctionDocumentation{
        .description = R"(
Decode a [Base32](https://datatracker.ietf.org/doc/html/rfc4648) encoded string. If the input string is not a valid Base32 return an empty string.)",
        .arguments = {
            {"arg", "A Base32 (rfc4648) encoded string"},
        },
        .examples = {
            {"simple_decoding1", "SELECT tryBase32Decode('ME======')", "a"},
            {"simple_decoding2", "SELECT tryBase32Decode('JBSWY3DP')", "Hello"},
            {"non_ascii", "SELECT hex(tryBase32Decode('4W2HIXV4'))", "E5B4745EBC"},
            {"invalid_base32", "SELECT tryBase32Decode('invalid_base32')", ""},
            {"empty_string", "SELECT tryBase32Decode('')", ""},
            {"non_base32_characters", "SELECT tryBase32Decode('12345')", ""},
        },
        .category = FunctionDocumentation::Category::String});
}
}
