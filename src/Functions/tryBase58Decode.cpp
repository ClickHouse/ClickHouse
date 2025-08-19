#include <Functions/FunctionBase58Conversion.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{
struct NameTryBase58Decode
{
    static constexpr auto name = "tryBase58Decode";
};

using TryBase58DecodeImpl = BaseXXDecode<Base58DecodeTraits, NameTryBase58Decode, BaseXXDecodeErrorHandling::ReturnEmptyString>;
using FunctionTryBase58Decode = FunctionBaseXXConversion<TryBase58DecodeImpl>;
}

REGISTER_FUNCTION(TryBase58Decode)
{
    FunctionDocumentation::Description description = R"(
Like `base58Decode`, but returns an empty string in case of error.
)";
    FunctionDocumentation::Syntax syntax = "tryBase58Decode(encoded)";
    FunctionDocumentation::Arguments arguments = {
        {"encoded", "[String](../data-types/string.md) column or constant. If the string is not valid Base58-encoded, returns an empty string in case of error.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string containing the decoded value of the argument.", {"String"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTryBase58Decode>(documentation);
}

}
