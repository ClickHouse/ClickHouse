#include <Functions/FunctionBase32Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

REGISTER_FUNCTION(Base32Encode)
{
    factory.registerFunction<FunctionBaseXXConversion<BaseXXEncode<Base32Traits>>>(FunctionDocumentation{
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
