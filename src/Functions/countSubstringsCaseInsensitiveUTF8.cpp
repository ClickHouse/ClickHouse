#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CountSubstringsImpl.h>


namespace DB
{
namespace
{

struct NameCountSubstringsCaseInsensitiveUTF8
{
    static constexpr auto name = "countSubstringsCaseInsensitiveUTF8";
};

using FunctionCountSubstringsCaseInsensitiveUTF8 = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitiveUTF8)
{
    FunctionDocumentation::Description description = R"(
Like [`countSubstrings`](#countSubstrings) but counts case-insensitively and assumes that haystack is a UTF-8 string.
    )";
    FunctionDocumentation::Syntax syntax = "countSubstringsCaseInsensitiveUTF8(haystack, needle[, start_pos])";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "UTF-8 string in which the search is performed.", {"String", "Enum"}},
        {"needle", "Substring to be searched.", {"String"}},
        {"start_pos", "Optional. Position (1-based) in `haystack` at which the search starts.", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of occurrences of the needle in the haystack.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА');",
        R"(
┌─countSubstri⋯шка', 'КА')─┐
│                        4 │
└──────────────────────────┘
        )"
    },
    {
        "With start_pos argument",
        "SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА', 13);",
        R"(
┌─countSubstri⋯, 'КА', 13)─┐
│                        2 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionCountSubstringsCaseInsensitiveUTF8>(documentation);
}
}
