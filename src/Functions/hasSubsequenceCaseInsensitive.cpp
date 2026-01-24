#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveASCII
{
    static constexpr bool is_utf8 = false;

    static int toLowerIfNeed(int c) { return std::tolower(c); }
};

struct NameHasSubsequenceCaseInsensitive
{
    static constexpr auto name = "hasSubsequenceCaseInsensitive";
};

using FunctionHasSubsequenceCaseInsensitive = HasSubsequenceImpl<NameHasSubsequenceCaseInsensitive, HasSubsequenceCaseInsensitiveASCII>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitive)
{
    FunctionDocumentation::Description description = "Like [`hasSubsequence`](#hasSubsequence) but searches case-insensitively.";
    FunctionDocumentation::Syntax syntax = "hasSubsequenceCaseInsensitive(haystack, needle)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String"}},
        {"needle", "Subsequence to be searched.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1, if needle is a subsequence of haystack, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT hasSubsequenceCaseInsensitive('garbage', 'ARG');",
        R"(
┌─hasSubsequenceCaseInsensitive('garbage', 'ARG')─┐
│                                               1 │
└─────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasSubsequenceCaseInsensitive>(documentation, FunctionFactory::Case::Insensitive);
}

}
