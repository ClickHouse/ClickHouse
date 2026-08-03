#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceRegexpImpl.h>


namespace DB
{
namespace
{

struct NameReplaceRegexpOne
{
    static constexpr auto name = "replaceRegexpOne";
};

using FunctionReplaceRegexpOne = FunctionStringReplace<ReplaceRegexpImpl<NameReplaceRegexpOne, ReplaceRegexpTraits::First>, NameReplaceRegexpOne>;

}

REGISTER_FUNCTION(ReplaceRegexpOne)
{
    FunctionDocumentation::Description description = R"(
Replaces the first occurrence of the substring matching the regular expression `pattern` (in re2 syntax) in `haystack` by the `replacement` string.
`replacement` can contain substitutions `\0-\9`.
Substitutions `\1-\9` correspond to the 1st to 9th capturing group (submatch), substitution `\0` corresponds to the entire match.
To use a verbatim `\` character in the `pattern` or `replacement` strings, escape it using `\`.
Also keep in mind that string literals require extra escaping.
)";
    FunctionDocumentation::Syntax syntax = "replaceRegexpOne(haystack, pattern, replacement)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The input string to search in.", {"String"}},
        {"pattern", "The regular expression pattern to find.", {"const String"}},
        {"replacement", "The string to replace the pattern with, may contain substitutions.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with the first regex match replaced.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Converting ISO dates to American format",
        R"(
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
        )",
        R"(
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
        )"
    },
    {
        "Copying a string ten times",
        R"(SELECT replaceRegexpOne('Hello, World!', '.*', '\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0\\\\0') AS res)",
        R"(
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReplaceRegexpOne>(documentation);
}

}
