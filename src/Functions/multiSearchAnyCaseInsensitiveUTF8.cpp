#include <Functions/FunctionsMultiStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAnyCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAnyCaseInsensitiveUTF8";
};

using FunctionMultiSearchCaseInsensitiveUTF8
    = FunctionsMultiStringSearch<MultiSearchImpl<NameMultiSearchAnyCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAnyCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [multiSearchAnyUTF8](#multiSearchAnyUTF8) but ignores case.
    )";
    FunctionDocumentation::Syntax syntax = "multiSearchAnyCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String"}},
        {"needle", "UTF-8 substrings to be searched.", {"Array(String)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1`, if there was at least one case-insensitive match, otherwise `0`, if there was not at least one case-insensitive match.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Given a UTF-8 string 'Здравствуйте', check if character 'з' (lowercase) is present",
        "SELECT multiSearchAnyCaseInsensitiveUTF8('Здравствуйте',['з'])",
        R"(
┌─multiSearchA⋯те', ['з'])─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMultiSearchCaseInsensitiveUTF8>(documentation);
}

}
