#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsUTF8
{
    static constexpr auto name = "multiSearchAllPositionsUTF8";
};

using FunctionMultiSearchAllPositionsUTF8
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`multiSearchAllPositions`](#multiSearchAllPositions) but assumes `haystack` and the `needle` substrings are UTF-8 encoded strings.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAllPositionsUTF8(haystack, needle1[, needle2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 encoded string in which the search is performed.", {"String"}},
        {"needle1[, needle2, ...]", "An array of UTF-8 encoded substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns array of the starting position in bytes and counting from 1 (if the substring was found), `0` if the substring was not found.", {"Array"}};
    FunctionDocumentation::Examples examples = {
    {
        "UTF-8 multi-search",
        "SELECT multiSearchAllPositionsUTF8('ClickHouse',['C','H'])",
        R"(
┌─multiSearchAllPositionsUTF8('ClickHouse', ['C', 'H'])─┐
│ [1,6]                                                 │
└───────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchAllPositionsUTF8>(documentation);
}

}
