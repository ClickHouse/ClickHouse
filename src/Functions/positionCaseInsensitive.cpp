#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};

using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(PositionCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Like [`position`](#position) but case-insensitive.
    )";
    FunctionDocumentation::Syntax syntax = "positionCaseInsensitive(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns starting position in bytes and counting from 1, if the substring was found, otherwise `0`, if the substring was not found.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Case insensitive search",
        "SELECT positionCaseInsensitive('Hello, world!', 'hello')",
        R"(
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPositionCaseInsensitive>(documentation);
    factory.registerAlias("instr", NamePositionCaseInsensitive::name, FunctionFactory::Case::Insensitive);
}
}
