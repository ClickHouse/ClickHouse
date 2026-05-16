#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/MatchImpl.h>

namespace DB
{
struct NameSimilarTo
{
    static constexpr auto name = "similarTo";
};

using SimilarToImpl = MatchImpl<NameSimilarTo, MatchTraits::Syntax::SimilarTo, MatchTraits::Case::Sensitive, MatchTraits::Result::DontNegate>;
using FunctionSimilarTo = FunctionsStringSearch<SimilarToImpl>;


REGISTER_FUNCTION(SimilarTo)
{
    FunctionDocumentation::Description description = R"(
Returns whether string `haystack` matches the `SIMILAR TO` expression `pattern`.
   )";
    FunctionDocumentation::Syntax syntax = R"(
similarTo(haystack, pattern)
-- haystack SIMILAR TO pattern
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "`SIMILAR TO` pattern to match against. Can contain `%` (matches any number of characters), `_` (matches single character), `\\` for escaping, and regular expression metacharacters except `^`, `$` and `.`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string matches the `SIMILAR TO` pattern, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT similarTo('ClickHouse', 'Cl_ck[hH]ouse');",
        R"(
┌─similarTo('ClickHouse', 'Cl_ck[hH]ouse')─┐
│                                        1 │
└──────────────────────────────────────────┘
        )"
    },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSimilarTo>(documentation);
}

}
