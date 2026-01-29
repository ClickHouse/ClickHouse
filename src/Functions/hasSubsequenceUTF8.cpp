#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>


namespace DB
{
namespace
{

struct HasSubsequenceCaseSensitiveUTF8
{
    static constexpr bool is_utf8 = true;

    static int toLowerIfNeed(int code_point) { return code_point; }
};

struct NameHasSubsequenceUTF8
{
    static constexpr auto name = "hasSubsequenceUTF8";
};

using FunctionHasSubsequenceUTF8 = HasSubsequenceImpl<NameHasSubsequenceUTF8, HasSubsequenceCaseSensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`hasSubsequence`](/sql-reference/functions/string-search-functions#hasSubsequence) but assumes haystack and needle are UTF-8 encoded strings.
    )";
    FunctionDocumentation::Syntax syntax = R"(
        hasSubsequenceUTF8(haystack, needle)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The string in which to search.", {"String"}},
        {"needle", "The subsequence to search for.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `needle` is a subsequence of `haystack`, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT hasSubsequenceUTF8('картошка', 'кошка');",
        R"(
┌─hasSubsequen⋯', 'кошка')─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "Non-matching subsequence",
        "SELECT hasSubsequenceUTF8('картошка', 'апельсин');",
        R"(
┌─hasSubsequen⋯'апельсин')─┐
│                        0 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasSubsequenceUTF8>(documentation, FunctionFactory::Case::Insensitive);
}

}
