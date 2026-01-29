#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstPositionImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitiveUTF8";
};

using FunctionMultiSearchFirstPositionCaseInsensitiveUTF8 = FunctionsMultiStringSearch<
    MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstPositionCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchFirstPosition](#multiSearchFirstPosition) but assumes `haystack` and `needle` to be UTF-8 strings and ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String"}},
        {"needle", "Array of UTF-8 substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings, ignoring case. Returns `0`, if there was no match.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Find the leftmost offset in UTF-8 string 'Здравствуй, мир' ('Hello, world') which matches any of the given needles",
        "SELECT multiSearchFirstPositionCaseInsensitiveUTF8('Здравствуй, мир', ['МИР', 'вст', 'Здра'])",
        R"(
┌─multiSearchFirstPositionCaseInsensitiveUTF8('Здравствуй, мир', ['мир', 'вст', 'Здра'])─┐
│                                                                                      3 │
└────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitiveUTF8>(documentation);
}

}
