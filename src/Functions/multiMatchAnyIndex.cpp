#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiMatchAnyImpl.h>


namespace DB
{
namespace
{

struct NameMultiMatchAnyIndex
{
    static constexpr auto name = "multiMatchAnyIndex";
};

using FunctionMultiMatchAnyIndex = FunctionsMultiStringSearch<MultiMatchAnyImpl<NameMultiMatchAnyIndex, /*ResultType*/ UInt64, MultiMatchTraits::Find::AnyIndex, /*WithEditDistance*/ false>>;

}

REGISTER_FUNCTION(MultiMatchAnyIndex)
{
    FunctionDocumentation::Description description = "Like [`multiMatchAny`](#multiMatchAny) but returns any index that matches the haystack.";
    FunctionDocumentation::Syntax syntax = "multiMatchAnyIndex(haystack, [pattern1, pattern2, ..., patternn])";
    FunctionDocumentation::Arguments arguments =
    {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"pattern", "Regular expressions to match against.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (starting from 1) of the first pattern that matches, or 0 if no match is found.", {"UInt64"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiMatchAnyIndex('ClickHouse', ['[0-9]', 'House', 'Click']);",
        R"(
┌─multiMatchAn⋯, 'Click'])─┐
│                        3 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiMatchAnyIndex>(documentation);
}

}
