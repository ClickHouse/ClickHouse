#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstIndexImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitive";
};

using FunctionMultiSearchFirstIndexCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Returns the index `i` (starting from 1) of the leftmost found needle_i in the string `haystack` and 0 otherwise.
Ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstIndexCaseInsensitive(haystack, [needle1, needle2, ..., needleN]";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle", "Substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (starting from 1) of the leftmost found needle. Otherwise `0`, if there was no match.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT multiSearchFirstIndexCaseInsensitive('hElLo WoRlD', ['World', 'Hello']);",
        R"(
┌─multiSearchF⋯, 'Hello'])─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitive>(documentation);
}

}
