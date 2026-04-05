#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceRegexpImpl.h>


namespace DB
{
namespace
{

struct NameReplaceRegexpAll
{
    static constexpr auto name = "replaceRegexpAll";
};

using FunctionReplaceRegexpAll = FunctionStringReplace<ReplaceRegexpImpl<NameReplaceRegexpAll, ReplaceRegexpTraits::All>, NameReplaceRegexpAll>;

}

REGISTER_FUNCTION(ReplaceRegexpAll)
{
    FunctionDocumentation::Description description = R"(
Like `replaceRegexpOne` but replaces all occurrences of the pattern.
As an exception, if a regular expression worked on an empty substring, the replacement is not made more than once.
)";
    FunctionDocumentation::Syntax syntax = "replaceRegexpAll(haystack, pattern, replacement)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The input string to search in.", {"String"}},
        {"pattern", "The regular expression pattern to find.", {"const String"}},
        {"replacement", "The string to replace the pattern with, may contain substitutions.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with all regex matches replaced.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Replace all characters with doubled version",
        R"(SELECT replaceRegexpAll('Hello123', '.', '\\\\0\\\\0') AS res)",
        R"(
┌─res──────────────────┐
│ HHeelllloo112233     │
└──────────────────────┘
        )"
    },
    {
        "Empty substring replacement example",
        "SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res",
        R"(
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReplaceRegexpAll>(documentation);
    factory.registerAlias("REGEXP_REPLACE", NameReplaceRegexpAll::name, FunctionFactory::Case::Insensitive);
}

}
