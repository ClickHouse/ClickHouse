#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePositionCaseInsensitiveUTF8
{
    static constexpr auto name = "positionCaseInsensitiveUTF8";
};

using FunctionPositionCaseInsensitiveUTF8
    = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(PositionCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`positionUTF8`](#positionUTF8) but searches case-insensitively.
    )";
    FunctionDocumentation::Syntax syntax = "positionCaseInsensitiveUTF8(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns starting position in bytes and counting from 1, if the substring was found, otherwise `0`, if the substring was not found.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Case insensitive UTF-8 search",
        "SELECT positionCaseInsensitiveUTF8('Привет мир', 'МИР')",
        R"(
┌─positionCaseInsensitiveUTF8('Привет мир', 'МИР')─┐
│                                                8 │
└──────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>(documentation);
}

}
