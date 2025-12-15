#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitive
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitive";
};

using FunctionMultiSearchAllPositionsCaseInsensitive
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Like [`multiSearchAllPositions`](#multiSearchAllPositions) but ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAllPositionsCaseInsensitive(haystack, needle1[, needle2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle1[, needle2, ...]", "An array of one or more substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns array of the starting position in bytes and counting from 1 (if the substring was found), `0` if the substring was not found.", {"Array(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Case insensitive multi-search",
        "SELECT multiSearchAllPositionsCaseInsensitive('ClickHouse',['c','h'])",
        R"(
┌─multiSearchA⋯['c', 'h'])─┐
│ [1,6]                    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitive>(documentation);
}

}
