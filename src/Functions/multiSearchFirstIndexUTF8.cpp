#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchFirstIndexImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multiSearchFirstIndexUTF8";
};

using FunctionMultiSearchFirstIndexUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstIndexUTF8)
{
    FunctionDocumentation::Description description = R"(
Returns the index `i` (starting from 1) of the leftmost found needle_i in the string `haystack` and 0 otherwise.
Assumes `haystack` and `needle` are UTF-8 encoded strings.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchFirstIndexUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String"}},
        {"needle", "Array of UTF-8 substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the index (starting from 1) of the leftmost found needle. Otherwise 0, if there was no match.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT multiSearchFirstIndexUTF8('Здравствуйте мир', ['мир', 'здравствуйте']);",
        R"(
┌─multiSearchF⋯вствуйте'])─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>(documentation);
}

}
