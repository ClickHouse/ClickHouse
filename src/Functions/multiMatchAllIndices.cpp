#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAllIndicesImpl.h>


namespace DB
{
namespace
{

struct NameMultiMatchAllIndices
{
    static constexpr auto name = "multiMatchAllIndices";
};

using FunctionMultiMatchAllIndices = FunctionsMultiStringSearch<MultiMatchAllIndicesImpl<NameMultiMatchAllIndices, /*ResultType*/ UInt64, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAllIndices)
{
    FunctionDocumentation::Description description = "Like [`multiMatchAny`](#multiMatchAny) but returns the array of all indices that match the haystack in any order.";
    FunctionDocumentation::Syntax syntax = "multiMatchAllIndices(haystack, [pattern1, pattern2, ..., patternn])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"pattern", "Regular expressions to match against.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Array of all indices (starting from 1) that match the haystack in any order. Returns an empty array if no matches are found.", {"Array(UInt64)"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiMatchAllIndices('ClickHouse', ['[0-9]', 'House', 'Click', 'ouse']);",
        R"(
┌─multiMatchAl⋯', 'ouse'])─┐
│ [3, 2, 4]                │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiMatchAllIndices>(documentation);
}

}
