#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static int toLowerIfNeed(int c) { return c; }
};

struct NameHasSubsequence
{
    static constexpr auto name = "hasSubsequence";
};

using FunctionHasSubsequence = HasSubsequenceImpl<NameHasSubsequence, HasSubsequenceCaseSensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequence)
{
    FunctionDocumentation::Description description = R"(
Checks if a needle is a subsequence of a haystack.
A subsequence of a string is a sequence that can be derived from another string by deleting some or no characters without changing the order of the remaining characters.
    )";
    FunctionDocumentation::Syntax syntax = "hasSubsequence(haystack, needle)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which to search for the subsequence.", {"String"}},
        {"needle", "Subsequence to be searched.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if needle is a subsequence of haystack, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic subsequence check",
        "SELECT hasSubsequence('Hello World', 'HlWrd')",
        R"(
┌─hasSubsequence('Hello World', 'HlWrd')─┐
│                                      1 │
└────────────────────────────────────────┘
        )"
    },
    {
        "No subsequence found",
        "SELECT hasSubsequence('Hello World', 'xyz')",
        R"(
┌─hasSubsequence('Hello World', 'xyz')─┐
│                                    0 │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasSubsequence>(documentation, FunctionFactory::Case::Insensitive);
}

}
