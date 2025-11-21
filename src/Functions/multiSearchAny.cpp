#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAny
{
    static constexpr auto name = "multiSearchAny";
};

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAny, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAny)
{
    FunctionDocumentation::Description description = R"(
Checks if at least one of a number of needle strings matches the haystack string.

Functions [`multiSearchAnyCaseInsensitive`](#multiSearchAnyCaseInsensitive), [`multiSearchAnyUTF8`](#multiSearchAnyUTF8) and [`multiSearchAnyCaseInsensitiveUTF8`](#multiSearchAnyCaseInsensitiveUTF8) provide case-insensitive and/or UTF-8 variants of this function.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAny(haystack, needle1[, needle2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle1[, needle2, ...]", "An array of substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1`, if there was at least one match, otherwise `0`, if there was not at least one match.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Any match search",
        "SELECT multiSearchAny('ClickHouse',['C','H'])",
        R"(
┌─multiSearchAny('ClickHouse', ['C', 'H'])─┐
│                                        1 │
└──────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearch>(documentation);
}

}
