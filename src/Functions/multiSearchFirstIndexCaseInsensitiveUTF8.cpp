#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstIndexImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitiveUTF8";
};

using FunctionMultiSearchFirstIndexCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = "Searches for multiple needle strings in a haystack string, case-insensitively with UTF-8 encoding support, and returns the 1-based index of the first needle found.";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstIndexCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The string to search in.", {"String"}},
        {"needles", "Array of strings to search for.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value =
    {
        "Returns the 1-based index (position in the needles array) of the first needle found in the haystack. Returns 0 if no needles are found. The search is case-insensitive and respects UTF-8 character encoding.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiSearchFirstIndexCaseInsensitiveUTF8('ClickHouse Database', ['CLICK', 'data', 'server']);",
        R"(
┌─multiSearchF⋯ 'server'])─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "UTF-8 case handling",
        "SELECT multiSearchFirstIndexCaseInsensitiveUTF8('Привет Мир', ['мир', 'ПРИВЕТ']);",
        R"(
┌─multiSearchF⋯ 'ПРИВЕТ'])─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "No match found",
        "SELECT multiSearchFirstIndexCaseInsensitiveUTF8('Hello World', ['goodbye', 'test']);",
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
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitiveUTF8>(documentation);
}

}
