#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitive
{
    static constexpr auto name = "multiSearchAnyCaseInsensitive";
};
using FunctionMultiSearchCaseInsensitive = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchAny](#multiSearchAny) but ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAnyCaseInsensitive(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle", "Substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1`, if there was at least one case-insensitive match, otherwise `0`, if there was not at least one case-insensitive match.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Case insensitive search",
        "SELECT multiSearchAnyCaseInsensitive('ClickHouse',['c','h'])",
        R"(
┌─multiSearchAnyCaseInsensitive('ClickHouse', ['c', 'h'])─┐
│                                                       1 │
└─────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchCaseInsensitive>(documentation);
}

}
