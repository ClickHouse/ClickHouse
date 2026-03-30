#include <Functions/similarTo.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

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
        {"pattern", "`SIMILAR TO` pattern to match against. Can contain `%` (matches any number of characters), `_` (matches single character), and `\\` for escaping, and regular expression metacharacters.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string matches the `SIMILAR TO` pattern, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT similarTo('ClickHouse', '%House');",
        R"(
┌─similarTo('ClickHouse', '%House')─┐
│                            1 │
└──────────────────────────────┘
        )"
    },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionSimilarTo>(documentation);
}

}
