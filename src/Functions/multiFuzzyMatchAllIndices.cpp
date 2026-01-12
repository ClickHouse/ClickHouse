#include <Functions/FunctionsMultiStringFuzzySearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAllIndicesImpl.h>


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAllIndices
{
    static constexpr auto name = "multiFuzzyMatchAllIndices";
};

using FunctionMultiFuzzyMatchAllIndices = FunctionsMultiStringFuzzySearch<MultiMatchAllIndicesImpl<NameMultiFuzzyMatchAllIndices, /*ResultType*/ UInt64, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAllIndices)
{
    FunctionDocumentation::Description description = "Like [`multiFuzzyMatchAny`](#multiFuzzyMatchAny) but returns the array of all indices in any order that match the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance).";
    FunctionDocumentation::Syntax syntax = "multiFuzzyMatchAllIndices(haystack, distance, [pattern1, pattern2, ..., patternN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"distance", "The maximum edit distance for fuzzy matching.", {"UInt8"}},
        {"pattern", "Array of patterns to match against.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of all indices (starting from 1) that match the haystack within the specified edit distance in any order. Returns an empty array if no matches are found.", {"Array(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT multiFuzzyMatchAllIndices('ClickHouse', 2, ['ClickHouse', 'ClckHouse', 'ClickHose', 'House']);",
        R"(
┌─multiFuzzyMa⋯, 'House'])─┐
│ [3,1,4,2]                │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiFuzzyMatchAllIndices>(documentation);
}

}
