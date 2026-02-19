#include <Functions/FunctionsMultiStringFuzzySearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAnyIndex
{
    static constexpr auto name = "multiFuzzyMatchAnyIndex";
};

using FunctionMultiFuzzyMatchAnyIndex = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAnyIndex, /*ResultType*/ UInt64, MultiMatchTraits::Find::AnyIndex, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAnyIndex)
{
    FunctionDocumentation::Description description = "Like [`multiFuzzyMatchAny`](#multiFuzzyMatchAny) but returns any index that matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance).";
    FunctionDocumentation::Syntax syntax = "multiFuzzyMatchAnyIndex(haystack, distance, [pattern1, pattern2, ..., patternn])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"distance", "The maximum edit distance for fuzzy matching.", {"UInt8"}},
        {"pattern", "Array of patterns to match against.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (starting from 1) of any pattern that matches the haystack within the specified edit distance, otherwise `0`.", {"UInt64"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiFuzzyMatchAnyIndex('ClickHouse', 2, ['ClckHouse', 'ClickHose', 'ClickHouse']);",
        R"(
┌─multiFuzzyMa⋯ickHouse'])─┐
│                        2 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiFuzzyMatchAnyIndex>(documentation);
}

}
