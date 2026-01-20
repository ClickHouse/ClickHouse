#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstPositionImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstPosition
{
    static constexpr auto name = "multiSearchFirstPosition";
};

using FunctionMultiSearchFirstPosition
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<NameMultiSearchFirstPosition, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstPosition)
{
    FunctionDocumentation::Description description = R"(
Like [`position`](#position) but returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings.

Functions [`multiSearchFirstPositionCaseInsensitive`](#multiSearchFirstPositionCaseInsensitive), [`multiSearchFirstPositionUTF8`](#multiSearchFirstPositionUTF8) and [`multiSearchFirstPositionCaseInsensitiveUTF8`](#multiSearchFirstPositionCaseInsensitiveUTF8) provide case-insensitive and/or UTF-8 variants of this function.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstPosition(haystack, needle1[, needle2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle1[, needle2, ...]", "An array of one or more substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings, otherwise `0`, if there was no match.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "First position search",
        "SELECT multiSearchFirstPosition('Hello World',['llo', 'Wor', 'ld'])",
        R"(
┌─multiSearchFirstPosition('Hello World', ['llo', 'Wor', 'ld'])─┐
│                                                             3 │
└───────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstPosition>(documentation);
}

}
