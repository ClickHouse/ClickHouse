#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>


namespace DB
{
namespace
{

struct NameMatchCaseInsensitive
{
    static constexpr auto name = "matchCaseInsensitive";
};

using FunctionMatchCaseInsensitive = FunctionsStringSearch<MatchImpl<NameMatchCaseInsensitive, MatchTraits::Syntax::Re2, MatchTraits::Case::Insensitive, MatchTraits::Result::DontNegate>>;

}

REGISTER_FUNCTION(MatchCaseInsensitive)
{
    FunctionDocumentation::Description description = R"(
Similar to [`match`](#match) but matches case-insensitively.

This function uses the RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.

Alternative operator syntax: `haystack ~* pattern` (PostgreSQL-style).
    )";
    FunctionDocumentation::Syntax syntax = R"(
matchCaseInsensitive(haystack, pattern)
-- haystack ~* pattern
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the pattern is searched.", {"String"}},
        {"pattern", "Regular expression pattern. Can be a constant or come from a column.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the pattern matches case-insensitively, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT matchCaseInsensitive('Hello World', 'hello.*'), 'Hello World' ~* 'HELLO.*'",
        R"(
┌─matchCaseInsensitive('Hello World', 'hello.*')─┬─matchCaseInsensitive('Hello World', 'HELLO.*')─┐
│                                              1 │                                              1 │
└────────────────────────────────────────────────┴────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMatchCaseInsensitive>(documentation);
}

}
