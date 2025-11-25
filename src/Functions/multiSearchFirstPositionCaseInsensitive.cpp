#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstPositionImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitive";
};

using FunctionMultiSearchFirstPositionCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstPositionCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchFirstPosition](#multiSearchFirstPosition) but ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstPositionCaseInsensitive(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle", "Array of substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings. Returns `0`, if there was no match.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Case insensitive first position",
        "SELECT multiSearchFirstPositionCaseInsensitive('HELLO WORLD',['wor', 'ld', 'ello'])",
        R"(
┌─multiSearchFirstPositionCaseInsensitive('HELLO WORLD', ['wor', 'ld', 'ello'])─┐
│                                                                             2 │
└───────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitive>(documentation);
}

}
