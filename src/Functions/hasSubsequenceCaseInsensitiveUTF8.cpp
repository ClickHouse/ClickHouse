#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>

#include <Poco/Unicode.h>

namespace DB
{
namespace
{

struct HasSubsequenceCaseInsensitiveUTF8
{
    static constexpr bool is_utf8 = true;

    static int toLowerIfNeed(int code_point) { return Poco::Unicode::toLower(code_point); }
};

struct NameHasSubsequenceCaseInsensitiveUTF8
{
    static constexpr auto name = "hasSubsequenceCaseInsensitiveUTF8";
};

using FunctionHasSubsequenceCaseInsensitiveUTF8 = HasSubsequenceImpl<NameHasSubsequenceCaseInsensitiveUTF8, HasSubsequenceCaseInsensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = "Like [`hasSubsequenceUTF8`](#hasSubsequenceUTF8) but searches case-insensitively.";
    FunctionDocumentation::Syntax syntax = "hasSubsequenceCaseInsensitiveUTF8(haystack, needle)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF8-encoded string in which the search is performed.", {"String"}},
        {"needle", "UTF8-encoded subsequence string to be searched.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns 1, if needle is a subsequence of haystack, 0 otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT hasSubsequenceCaseInsensitiveUTF8('ClickHouse - столбцовая система управления базами данных', 'СИСТЕМА');",
        R"(
┌─hasSubsequen⋯ 'СИСТЕМА')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasSubsequenceCaseInsensitiveUTF8>(documentation, FunctionFactory::Case::Insensitive);
}

}
