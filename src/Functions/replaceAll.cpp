#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceStringImpl.h>


namespace DB
{
namespace
{

struct NameReplaceAll
{
    static constexpr auto name = "replaceAll";
};

using FunctionReplaceAll = FunctionStringReplace<ReplaceStringImpl<NameReplaceAll, ReplaceStringTraits::Replace::All>, NameReplaceAll>;

}

REGISTER_FUNCTION(ReplaceAll)
{
    FunctionDocumentation::Description description = R"(
Replaces all occurrences of the substring `pattern` in `haystack` by the `replacement` string.
)";
    FunctionDocumentation::Syntax syntax = "replaceAll(haystack, pattern, replacement)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The input string to search in.", {"String"}},
        {"pattern", "The substring to find and replace.", {"const String"}},
        {"replacement", "The string to replace the pattern with.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with all occurrences of pattern replaced.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Replace all occurrences",
        "SELECT replaceAll('Hello, Hello world', 'Hello', 'Hi') AS res;",
        R"(
┌─res──────────┐
│ Hi, Hi world │
└──────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReplaceAll>(documentation);
    factory.registerAlias("replace", NameReplaceAll::name, FunctionFactory::Case::Insensitive);
}

}
