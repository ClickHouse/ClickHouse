#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase32Decode
{
    static constexpr auto name = "base32Decode";
};

using Base32DecodeImpl = BaseXXDecode<Base32DecodeTraits, NameBase32Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase32Decode = FunctionBaseXXConversion<Base32DecodeImpl>;
}

REGISTER_FUNCTION(Base32Decode)
{
    factory.registerFunction<FunctionBase32Decode>(FunctionDocumentation{
        .description = R"(
Decode a [Base32](https://datatracker.ietf.org/doc/html/rfc4648) encoded string. The input string must be a valid Base32 encoded string, otherwise an exception will be thrown.)",
        .arguments = {
            {"arg", "A Base32 (rfc4648) encoded string"},
        },
        .examples = {
            {"simple_decoding1", "SELECT base32Decode('ME======')", "a"},
            {"simple_decoding2", "SELECT base32Decode('JBSWY3DP')", "Hello"},
            {"empty_string", "SELECT base32Decode('')", ""},
            {"non_ascii", "SELECT hex(base32Decode('4W2HIXV4'))", "E5B4745EBC"},
        },
        .category = FunctionDocumentation::Category::String});
}
}
