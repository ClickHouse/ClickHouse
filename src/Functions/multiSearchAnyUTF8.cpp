#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAnyUTF8
{
    static constexpr auto name = "multiSearchAnyUTF8";
};
using FunctionMultiSearchUTF8 = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAnyUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchAny](#multiSearchAny) but assumes `haystack` and the `needle` substrings are UTF-8 encoded strings.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAnyUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String"}},
        {"needle", "UTF-8 substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1`, if there was at least one match, otherwise `0`, if there was not at least one match.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Given ClickHouse as a UTF-8 string, check if there are any C ('\x43') or H ('\x48') letters in the word",
        R"(
SELECT multiSearchAnyUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x43','\x48'])
        )",
        R"(
┌─multiSearchAnyUTF8('ClickHouse', ['C', 'H'])─┐
│                                            1 │
└──────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchUTF8>(documentation);
}

}
