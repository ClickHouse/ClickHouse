#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitiveUTF8";
};

using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = "Like [`multiSearchAllPositionsUTF8`](#multiSearchAllPositionsUTF8) but ignores case.";
    FunctionDocumentation::Syntax syntax = "multiSearchAllPositionsCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 encoded string in which the search is performed.", {"String"}},
        {"needle", "UTF-8 encoded substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Array of the starting position in bytes and counting from 1 (if the substring was found). Returns 0 if the substring was not found.", {"Array"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Case-insensitive UTF-8 search",
        "SELECT multiSearchAllPositionsCaseInsensitiveUTF8('Здравствуй, мир!', ['здравствуй', 'МИР']);",
        R"(
┌─multiSearchA⋯й', 'МИР'])─┐
│ [1, 13]                  │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>(documentation);
}

}
