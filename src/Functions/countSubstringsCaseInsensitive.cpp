#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CountSubstringsImpl.h>


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitive
{
    static constexpr auto name = "countSubstringsCaseInsensitive";
};

using FunctionCountSubstringsCaseInsensitive = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitive)
{
    FunctionDocumentation::Description description = "Like [`countSubstrings`](#countSubstrings) but counts case-insensitively.";
    FunctionDocumentation::Syntax syntax = "countSubstringsCaseInsensitive(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of occurrences of the neddle in the haystack.", {"UInt64"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT countSubstringsCaseInsensitive('AAAA', 'aa');",
        R"(
┌─countSubstri⋯AAA', 'aa')─┐
│                        2 │
└──────────────────────────┘
        )"
    },
    {
        "With start_pos argument",
        "SELECT countSubstringsCaseInsensitive('abc___ABC___abc', 'abc', 4);",
        R"(
┌─countSubstri⋯, 'abc', 4)─┐
│                        2 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCountSubstringsCaseInsensitive>(documentation);
}
}
