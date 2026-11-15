#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>


namespace DB
{
namespace
{

struct NameNotMatch
{
    static constexpr auto name = "notMatch";
};

using FunctionNotMatch = FunctionsStringSearch<MatchImpl<NameNotMatch, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::Negate>>;

}

REGISTER_FUNCTION(NotMatch)
{
    FunctionDocumentation::Description description = R"(
Similar to [`match`](#match) but negates the result: checks that the string does not match the regular expression pattern.

This function uses the RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.

Alternative operator syntax: `haystack !~ pattern` (PostgreSQL-style).
    )";
    FunctionDocumentation::Syntax syntax = R"(
notMatch(haystack, pattern)
-- haystack !~ pattern
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the pattern is searched.", {"String"}},
        {"pattern", "Regular expression pattern. Can be a constant or come from a column.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0` if the pattern matches, `1` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT notMatch('Hello World', 'Hello.*'), 'Hello World' !~ 'goodbye.*'",
        R"(
┌─notMatch('Hello World', 'Hello.*')─┬─notMatch('Hello World', 'goodbye.*')─┐
│                                  0 │                                    1 │
└────────────────────────────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNotMatch>(documentation);
}

}
