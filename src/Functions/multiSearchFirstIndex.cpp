#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstIndexImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndex
{
    static constexpr auto name = "multiSearchFirstIndex";
};

using FunctionMultiSearchFirstIndex = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndex, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndex)
{
    FunctionDocumentation::Description description = "Searches for multiple needle strings in a haystack string (case-sensitive) and returns the 1-based index of the first needle found.";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstIndex(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The string to search in.", {"String"}},
        {"needles", "Array of strings to search for.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the 1-based index (position in the needles array) of the first needle found in the haystack. Returns 0 if no needles are found. The search is case-sensitive.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiSearchFirstIndex('ClickHouse Database', ['Click', 'Database', 'Server']);",
        R"(
┌─multiSearchF⋯ 'Server'])─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "Case-sensitive behavior",
        "SELECT multiSearchFirstIndex('ClickHouse Database', ['CLICK', 'Database', 'Server']);",
        R"(
┌─multiSearchF⋯ 'Server'])─┐
│                        2 │
└──────────────────────────┘
        )"
    },
    {
        "No match found",
        "SELECT multiSearchFirstIndex('Hello World', ['goodbye', 'test']);",
        R"(
┌─multiSearchF⋯', 'test'])─┐
│                        0 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstIndex>(documentation);
}

}
