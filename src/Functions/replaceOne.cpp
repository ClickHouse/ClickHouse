#include <Functions/FunctionStringReplace.h>
#include <Functions/FunctionFactory.h>
#include <Functions/ReplaceStringImpl.h>


namespace DB
{
namespace
{

struct NameReplaceOne
{
    static constexpr auto name = "replaceOne";
};

using FunctionReplaceOne = FunctionStringReplace<ReplaceStringImpl<NameReplaceOne, ReplaceStringTraits::Replace::First>, NameReplaceOne>;

}

REGISTER_FUNCTION(ReplaceOne)
{
    FunctionDocumentation::Description description = R"(
Replaces the first occurrence of the substring `pattern` in `haystack` by the `replacement` string.
    )";
    FunctionDocumentation::Syntax syntax = "replaceOne(haystack, pattern, replacement)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The input string to search in.", {"String"}},
        {"pattern", "The substring to find and replace.", {"const String"}},
        {"replacement", "The string to replace the pattern with.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a string with the first occurrence of pattern replaced.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Replace first occurrence",
        "SELECT replaceOne('Hello, Hello world', 'Hello', 'Hi') AS res;",
        R"(
┌─res─────────────┐
│ Hi, Hello world │
└─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringReplacement;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionReplaceOne>(documentation);
}

}
