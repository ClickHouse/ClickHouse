#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstPositionImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionUTF8
{
    static constexpr auto name = "multiSearchFirstPositionUTF8";
};

using FunctionMultiSearchFirstPositionUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstPositionUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchFirstPosition](#multiSearchFirstPosition) but assumes `haystack` and `needle` to be UTF-8 strings.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstPositionUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String"}},
        {"needle", "Array of UTF-8 substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Leftmost offset in a `haystack` string which matches any of multiple `needle` strings. Returns `0`, if there was no match.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Find the leftmost offset in UTF-8 string 'Здравствуй, мир' ('Hello, world') which matches any of the given needles",
        "SELECT multiSearchFirstPositionUTF8('Здравствуй, мир',['мир', 'вст', 'авст'])",
        R"(
┌─multiSearchFirstPositionUTF8('Здравствуй, мир', ['мир', 'вст', 'авст'])─┐
│                                                                       3 │
└─────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstPositionUTF8>(documentation);
}

}
