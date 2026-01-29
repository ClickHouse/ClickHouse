#include <Functions/FunctionsMultiStringFuzzySearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>


namespace DB
{
namespace
{

struct NameMultiFuzzyMatchAny
{
    static constexpr auto name = "multiFuzzyMatchAny";
};

using FunctionMultiFuzzyMatchAny = FunctionsMultiStringFuzzySearch<MultiMatchAnyImpl<NameMultiFuzzyMatchAny, /*ResultType*/ UInt8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ true>>;

}

REGISTER_FUNCTION(MultiFuzzyMatchAny)
{
    FunctionDocumentation::Description description = R"(
Like [`multiMatchAny`](#multiMatchAny) but returns 1 if any pattern matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance).
This function relies on the experimental feature of [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) library, and can be slow for some edge cases.
The performance depends on the edit distance value and patterns used, but it's always more expensive compared to non-fuzzy variants.

:::note
`multiFuzzyMatch*()` function family do not support UTF-8 regular expressions (it treats them as a sequence of bytes) due to restrictions of hyperscan.
:::
    )";
    FunctionDocumentation::Syntax syntax = R"(
        multiFuzzyMatchAny(haystack, distance, [pattern1, pattern2, ..., patternN])
    )";
    FunctionDocumentation::Arguments arguments =
    {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"distance", "The maximum edit distance for fuzzy matching.", {"UInt8"}},
        {"pattern", "Optional. An array of patterns to match against.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if any pattern matches the haystack within the specified edit distance, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiFuzzyMatchAny('ClickHouse', 2, ['ClickHouse', 'ClckHouse', 'ClickHose']);",
        R"(
┌─multiFuzzyMa⋯lickHose'])─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiFuzzyMatchAny>(documentation);
}

}
