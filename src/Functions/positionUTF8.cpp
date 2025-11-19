#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};

using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<NamePositionUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(PositionUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`position`](#position) but assumes `haystack` and `needle` are UTF-8 encoded strings.
    )";
    FunctionDocumentation::Syntax syntax = "positionUTF8(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns starting position in bytes and counting from 1, if the substring was found, otherwise `0`, if the substring was not found.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "UTF-8 character counting",
        "SELECT positionUTF8('Motörhead', 'r')",
        R"(
┌─position('Motörhead', 'r')─┐
│                          5 │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPositionUTF8>(documentation);
}

}
