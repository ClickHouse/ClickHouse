#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/MatchImpl.h>

namespace DB
{
struct NameNotSimilarTo
{
    static constexpr auto name = "notSimilarTo";
};

using NotSimilarToImpl = MatchImpl<NameNotSimilarTo, MatchTraits::Syntax::SimilarTo, MatchTraits::Case::Sensitive, MatchTraits::Result::Negate>;
using FunctionNotSimilarTo = FunctionsStringSearch<NotSimilarToImpl>;


REGISTER_FUNCTION(NotSimilarTo)
{
    FunctionDocumentation::Description description = R"(
Similar to [`similarTo`](#similarTo) but negates the result.
   )";
    FunctionDocumentation::Syntax syntax = R"(
notSimilarTo(haystack, pattern)
-- haystack NOT SIMILAR TO pattern
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "`SIMILAR TO` pattern to match against. Can contain `%` (matches any number of characters), `_` (matches single character), `\\` for escaping, and regular expression metacharacters except `^`, `$` and `.`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string does not match the `SIMILAR TO` pattern, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT notSimilarTo('ClickHouse', 'Cl_ck[0-9]ouse');",
        R"(
┌─notSimilarTo('ClickHouse', 'Cl_ck[0-9]ouse')─┐
│                                            1 │
└──────────────────────────────────────────────┘
        )"
    },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNotSimilarTo>(documentation);
}

}
