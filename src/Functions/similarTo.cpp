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

A `SIMILAR TO` expression can contain normal characters and the following metasymbols:

- `%` indicates an arbitrary number of arbitrary characters (including zero characters).
- `_` indicates a single arbitrary character.
- `|`, `*`, `+`, `?`, `{`, `}`, `(`, `)`, `[` and `]` have their usual regular expression meaning.
- `\` is for escaping any of the above and itself.

The regular expression metacharacters `^`, `$` and `.` are not part of the `SIMILAR TO` grammar and
denote the corresponding literal characters outside bracket expressions. Inside a bracket expression,
a leading `^` keeps its usual meaning and negates the class, e.g. `[^aeiou]` matches a single
character which is not a vowel. The pattern must match the whole `haystack`, not a substring of it.

## ESCAPE clause

The optional `ESCAPE` clause specifies a custom escape character (must be a single ASCII character).
When provided, the custom escape character replaces the default backslash for escaping the
metasymbols listed above, and the backslash loses its special meaning (i.e. it is treated as a
literal character).

The escape character must be followed by a `SIMILAR TO` special character or by itself; anything else
is an error, since an escape before an ordinary character would have no effect. Inside a bracket
expression such as `[...]` the escape character is not special and denotes itself, following the
POSIX and PostgreSQL behavior.
   )";
    FunctionDocumentation::Syntax syntax = R"(
similarTo(haystack, pattern[, escape_character])
-- haystack SIMILAR TO pattern [ESCAPE 'escape_character']
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "`SIMILAR TO` pattern to match against. Can contain `%` (matches any number of characters), `_` (matches single character), `\\` for escaping, and regular expression metacharacters except `^`, `$` and `.`, which are literal outside bracket expressions (a leading `^` inside `[...]` still negates the class).", {"String"}},
        {"escape_character", "Optional single-character string to use as the escape character instead of `\\`. Default: `\\`.", {"String"}}
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSimilarTo>(documentation);
}

}
