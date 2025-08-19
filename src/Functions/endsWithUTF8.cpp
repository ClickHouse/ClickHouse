#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStartsEndsWith.h>


namespace DB
{

using FunctionEndsWithUTF8 = FunctionStartsEndsWith<NameEndsWithUTF8>;

REGISTER_FUNCTION(EndsWithUTF8)
{
    FunctionDocumentation::Description description = R"(
Returns whether string `str` ends with `suffix`.
Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.
)";
    FunctionDocumentation::Syntax syntax = "endsWithUTF8(str, suffix)";
    FunctionDocumentation::Arguments arguments = {
        {"str", "String to check.", {"String"}},
        {"suffix", "Suffix to check for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `str` ends with `suffix`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {};
    FunctionDocumentation::IntroducedIn introduced_in = {};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionEndsWithUTF8>(documentation);
}

}
