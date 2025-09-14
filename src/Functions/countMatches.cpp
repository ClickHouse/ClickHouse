#include <Functions/FunctionFactory.h>
#include <Functions/countMatches.h>

namespace
{

struct FunctionCountMatchesCaseSensitive
{
    static constexpr auto name = "countMatches";
    static constexpr bool case_insensitive = false;
};
struct FunctionCountMatchesCaseInsensitive
{
    static constexpr auto name = "countMatchesCaseInsensitive";
    static constexpr bool case_insensitive = true;
};

}

namespace DB
{

REGISTER_FUNCTION(CountMatches)
{
    FunctionDocumentation::Description description_case_sensitive = R"(
Returns number of matches of a regular expression in a string.

:::note Version dependent behavior
The behavior of this function depends on the ClickHouse version:

- in versions < v25.6, the function stops counting at the first empty match even if a pattern accepts.
- in versions >= 25.6, the function continues execution when an empty match occurs. The legacy behavior can be restored using setting `count_matches_stop_at_empty_match = true`;
:::

    )";
    FunctionDocumentation::Syntax syntax_case_sensitive = "countMatches(haystack, pattern)";
    FunctionDocumentation::Arguments arguments_case_sensitive = {
        {"haystack", "The string to search in.", {"String"}},
        {"pattern", "Regular expression pattern.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_case_sensitive = {"Returns the number of matches found.", {"UInt64"}};
    FunctionDocumentation::Examples examples_case_sensitive = {
    {
        "Count digit sequences",
        "SELECT countMatches('hello 123 world 456 test', '[0-9]+')",
        R"(
┌─countMatches('hello 123 world 456 test', '[0-9]+')─┐
│                                                   2 │
└─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_case_sensitive = {21, 1};
    FunctionDocumentation::Category category_case_sensitive = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_case_sensitive = {description_case_sensitive, syntax_case_sensitive, argument_case_sensitive, {}, returned_value_case_sensitive, examples_case_sensitive, introduced_in_case_sensitive, category_case_sensitive};

    FunctionDocumentation::Description description_case_insensitive = R"(
Like [`countMatches`](#countMatches) but performs case-insensitive matching.
    )";
    FunctionDocumentation::Syntax syntax_case_insensitive = "countMatchesCaseInsensitive(haystack, pattern)";
    FunctionDocumentation::Arguments arguments_case_insensitive = {
        {"haystack", "The string to search in.", {"String"}},
        {"pattern", "Regular expression pattern.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_case_insensitive = {"Returns the number of matches found.", {"UInt64"}};
    FunctionDocumentation::Examples examples_case_insensitive = {
    {
        "Case insensitive count",
        "SELECT countMatchesCaseInsensitive('Hello HELLO world', 'hello')",
        R"(
┌─countMatchesCaseInsensitive('Hello HELLO world', 'hello')─┐
│                                                         2 │
└───────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_case_insensitive = {21, 1};
    FunctionDocumentation::Category category_case_insensitive = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_case_insensitive = {description_case_insensitive, syntax_case_insensitive, arguments_case_insensitive, returned_value_case_insensitive, examples_case_insensitive, introduced_in_case_insensitive, category_case_insensitive};

    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseSensitive>>(documentation_case_sensitive);
    factory.registerFunction<FunctionCountMatches<FunctionCountMatchesCaseInsensitive>>(documentation_case_insensitive);
}

}
