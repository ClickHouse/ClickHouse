#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase32Encode
{
    static constexpr auto name = "base32Encode";
};

using Base32EncodeImpl = BaseXXEncode<Base32EncodeTraits, NameBase32Encode>;
using FunctionBase32Encode = FunctionBaseXXConversion<Base32EncodeImpl>;
}

REGISTER_FUNCTION(Base32Encode)
{
    factory.registerFunction<FunctionBase32Encode>(FunctionDocumentation{
        .description = R"(
Encode a string with [Base32](https://datatracker.ietf.org/doc/html/rfc4648) encoding.)",
        .arguments = {
            {"arg", "A string to be encoded"},
        },
        .examples = {
            {"simple_encoding1", "SELECT base32Encode('a')", "ME======"},
            {"simple_encoding2", "SELECT base32Encode('Hello')", "JBSWY3DP"}
        },
        .category = FunctionDocumentation::Category::String});
}
}
