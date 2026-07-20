#include <Functions/FunctionBase62Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameBase62Decode
{
    static constexpr auto name = "base62Decode";
};

using Base62DecodeImpl = BaseXXDecode<Base62DecodeTraits, NameBase62Decode, BaseXXDecodeErrorHandling::ThrowException>;
using FunctionBase62Decode = FunctionBaseXXConversion<Base62DecodeImpl>;
}

REGISTER_FUNCTION(Base62Decode)
{
    FunctionDocumentation::Description description = R"(
Decodes a [Base62](https://en.wikipedia.org/wiki/Base62) string encoded with the alphabet `0-9A-Za-z`.
Each leading `0` character of the input is decoded as a leading zero byte.
If the string is not valid Base62-encoded, an exception is thrown.
)";
    FunctionDocumentation::Syntax syntax = "base62Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant to decode.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT base62Decode('T8dgcjRGuYUueWht');",
        R"(
┌─base62Decode⋯GuYUueWht')─┐
│ Hello world!             │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionBase62Decode>(documentation);
}

}
