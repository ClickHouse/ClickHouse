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

This function uses the RE2 regular expression library. Please refer to [re2](https://github.com/google/re2/wiki/Syntax) for supported syntax.

Matching works under UTF-8 assumptions, e.g. `¥` uses two bytes internally but matching treats it as a single codepoint.
The regular expression must not contain NULL bytes.
If the haystack or the pattern are not valid UTF-8, the behavior is undefined.

Unlike re2's default behavior, `.` matches line breaks. To disable this, prepend the pattern with `(?-s)`.

The pattern is automatically anchored at both ends (as if the pattern started with '^' and ended with '$').

If you only like to find substrings, you can use functions [`like`](#like) or [`position`](#position) instead - they work much faster than this function.

Alternative operator syntax: `haystack REGEXP pattern`.
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
