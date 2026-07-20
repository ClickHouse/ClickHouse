#include <Functions/FunctionBase62Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameTryBase62Decode
{
    static constexpr auto name = "tryBase62Decode";
};

using TryBase62DecodeImpl = BaseXXDecode<Base62DecodeTraits, NameTryBase62Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase62Decode = FunctionBaseXXConversion<TryBase62DecodeImpl>;
}

REGISTER_FUNCTION(TryBase62Decode)
{
    FunctionDocumentation::Description description = R"(
Like [`base62Decode`](#base62Decode), but returns an empty string in case of error.
)";
    FunctionDocumentation::Syntax syntax = "tryBase62Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "String column or constant. If the string is not valid Base62-encoded, returns an empty string in case of error.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT tryBase62Decode('1RVU3aMpUa') AS res, tryBase62Decode('invalid!') AS res_invalid;",
        R"(
┌─res─────┬─res_invalid─┐
│ Encoded │             │
└─────────┴─────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTryBase62Decode>(documentation);
}

}
