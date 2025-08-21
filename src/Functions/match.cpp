#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>


namespace DB
{
namespace
{

struct NameMatch
{
    static constexpr auto name = "match";
};

using FunctionMatch = FunctionsStringSearch<MatchImpl<NameMatch, MatchTraits::Syntax::Re2, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>>;

}

REGISTER_FUNCTION(Match)
{
    FunctionDocumentation::Description description = R"(
Checks if a provided string matches the provided regular expression pattern.

The pattern works under UTF-8 assumptions. The pattern is automatically anchored at both ends (as if the pattern started with '^' and ended with '$').
This function uses RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.
    )";
    FunctionDocumentation::Syntax syntax = "match(haystack, pattern)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the pattern is searched.", {"String"}},
        {"pattern", "Regular expression pattern.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the pattern matches, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic pattern matching",
        "SELECT match('Hello World', 'Hello.*')",
        R"(
┌─match('Hello World', 'Hello.*')─┐
│                               1 │
└─────────────────────────────────┘
        )"
    },
    {
        "Pattern not matching",
        "SELECT match('Hello World', 'goodbye.*')",
        R"(
┌─match('Hello World', 'goodbye.*')─┐
│                                 0 │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionMatch>(documentation);
    factory.registerAlias("REGEXP_MATCHES", NameMatch::name, FunctionFactory::Case::Insensitive);
}

}
