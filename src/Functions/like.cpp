#include <Functions/like.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(Like)
{
    FunctionDocumentation::Description description = R"(
Returns whether string `haystack` matches the `LIKE` expression `pattern`.

A `LIKE` expression can contain normal characters and the following metasymbols:

- `%` indicates an arbitrary number of arbitrary characters (including zero characters).
- `_` indicates a single arbitrary character.
- `\` is for escaping literals `%`, `_` and `\`.

Matching is based on UTF-8, e.g. `_` matches the Unicode code point `¥` which is represented in UTF-8 using two bytes.

If the haystack or the `LIKE` expression are not valid UTF-8, the behavior is undefined.

No automatic Unicode normalization is performed, you can use the `normalizeUTF8*` functions for that.

To match against literal `%`, `_` and `\` (which are `LIKE` metacharacters), prepend them with a backslash: `\%`, `\_` and `\\`.
The backslash loses its special meaning (i.e. is interpreted literally) if it prepends a character different than `%`, `_` or `\`.

:::note
ClickHouse requires backslashes in strings [to be quoted as well](../syntax.md#string), so you would actually need to write `\\%`, `\\_` and `\\\\`.
:::

For `LIKE` expressions of the form `%needle%`, the function is as fast as the `position` function.
All other LIKE expressions are internally converted to a regular expression and executed with a performance similar to function `match`.

## ESCAPE clause

The optional `ESCAPE` clause specifies a custom escape character (must be a single ASCII character).
When provided, the custom escape character replaces the default backslash for escaping `%` and `_` metacharacters.
The escape character can escape three things: `%` (literal percent), `_` (literal underscore), and itself (literal escape character).
When a custom escape character is used, the backslash has no special meaning and is treated as a literal character.
   )";
    FunctionDocumentation::Syntax syntax = R"(
like(haystack, pattern[, escape_character])
-- haystack LIKE pattern [ESCAPE 'escape_character']
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "`LIKE` pattern to match against. Can contain `%` (matches any number of characters), `_` (matches single character), and `\\` for escaping.", {"String"}},
        {"escape_character", "Optional single-character string to use as the escape character instead of `\\`. Default: `\\`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string matches the `LIKE` pattern, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT like('ClickHouse', '%House');",
        R"(
┌─like('ClickHouse', '%House')─┐
│                            1 │
└──────────────────────────────┘
        )"
    },
    {
        "Single character wildcard",
        "SELECT like('ClickHouse', 'Click_ouse');",
        R"(
┌─like('ClickH⋯lick_ouse')─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "Non-matching pattern",
        "SELECT like('ClickHouse', '%SQL%');",
        R"(
┌─like('ClickHouse', '%SQL%')─┐
│                           0 │
└─────────────────────────────┘
        )"
    },
    {
        "ESCAPE clause",
        "SELECT '50%off' LIKE '50#%off' ESCAPE '#';",
        R"(
┌─like('50%off', '50#%off', '#')─┐
│                              1 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLike>(documentation);
}

}
