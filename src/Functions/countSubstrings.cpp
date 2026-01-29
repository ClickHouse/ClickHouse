#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CountSubstringsImpl.h>


namespace DB
{
namespace
{

struct NameCountSubstrings
{
    static constexpr auto name = "countSubstrings";
};

using FunctionCountSubstrings = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstrings, PositionCaseSensitiveASCII>>;

}

REGISTER_FUNCTION(CountSubstrings)
{
    FunctionDocumentation::Description description = "Returns how often a substring `needle` occurs in a string `haystack`.";
    FunctionDocumentation::Syntax syntax = "countSubstrings(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments =
    {
        {"haystack", "String in which the search is performed. [String](../../sql-reference/data-types/string.md) or [Enum](../../sql-reference/data-types/enum.md)."},
        {"needle", "Substring to be searched. [String](../../sql-reference/data-types/string.md)."},
        {"start_pos", "Position (1-based) in `haystack` at which the search starts. [UInt](../../sql-reference/data-types/int-uint.md). Optional."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The number of occurrences.", {"UInt64"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT countSubstrings('aaaa', 'aa');",
        R"(
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
        )"
    },
    {
        "With start_pos argument",
        "SELECT countSubstrings('abc___abc', 'abc', 4);",
        R"(
┌─countSubstrings('abc___abc', 'abc', 4)─┐
│                                      1 │
└────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCountSubstrings>(documentation, FunctionFactory::Case::Insensitive);
}
}
