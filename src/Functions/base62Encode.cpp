#include <Functions/FunctionBase62Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase62Encode
{
    static constexpr auto name = "base62Encode";
};

using Base62EncodeImpl = BaseXXEncode<Base62EncodeTraits, NameBase62Encode>;
using FunctionBase62Encode = FunctionBaseXXConversion<Base62EncodeImpl>;
}
REGISTER_FUNCTION(Base62Encode)
{
    FunctionDocumentation::Description description = R"(
Encodes a string using [Base62](https://en.wikipedia.org/wiki/Base62) encoding with the alphabet `0-9A-Za-z`.
Each leading zero byte of the input is encoded as a leading `0` character.
)";
    FunctionDocumentation::Syntax syntax = "base62Encode(plaintext)";
    FunctionDocumentation::Arguments arguments = {
        {"plaintext", "Plaintext to encode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the encoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base62Encode('ClickHouse');",
        R"(
┌─base62Encode('ClickHouse')─┐
│ 1agk8B30gH5Kj7             │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase62Encode>(documentation);
}

}
