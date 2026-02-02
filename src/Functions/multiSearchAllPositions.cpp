#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAllPositions
{
    static constexpr auto name = "multiSearchAllPositions";
};

using FunctionMultiSearchAllPositions
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositions, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAllPositions)
{
    FunctionDocumentation::Description description = R"(
Like [`position`](#position) but returns an array of positions (in bytes, starting at 1) for multiple `needle` substrings in a `haystack` string.

All `multiSearch*()` functions only support up to 2^8 needles.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAllPositions(haystack, needle1[, needle2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle1[, needle2, ...]", "An array of one or more substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns array of the starting position in bytes and counting from 1, if the substring was found, `0`, if the substring was not found.", {"Array(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Multiple needle search",
        "SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])",
        R"(
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchAllPositions>(documentation);
}

}
