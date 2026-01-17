#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryStringAndFragment.h>

namespace DB
{

struct NameCutQueryStringAndFragment { static constexpr auto name = "cutQueryStringAndFragment"; };
using FunctionCutQueryStringAndFragment = FunctionStringToString<CutSubstringImpl<ExtractQueryStringAndFragment<false>>, NameCutQueryStringAndFragment>;

REGISTER_FUNCTION(CutQueryStringAndFragment)
{
    /// cutQueryStringAndFragment documentation
    FunctionDocumentation::Description description_cutQueryStringAndFragment = R"(
Removes the query string and fragment identifier, including the question mark and number sign, from a URL.
    )";
    FunctionDocumentation::Syntax syntax_cutQueryStringAndFragment = "cutQueryStringAndFragment(url)";
    FunctionDocumentation::Arguments arguments_cutQueryStringAndFragment = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cutQueryStringAndFragment = {"Returns the URL with query string and fragment identifier removed.", {"String"}};
    FunctionDocumentation::Examples examples_cutQueryStringAndFragment = {
    {
        "Usage example",
        R"(
SELECT cutQueryStringAndFragment('http://example.com/path?query=value&param=123#fragment');
        )",
        R"(
┌─cutQueryStringAndFragment('http://example.com/path?query=value&param=123#fragment')─┐
│ http://example.com/path                                                             │
└─────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cutQueryStringAndFragment = {1, 1};
    FunctionDocumentation::Category category_cutQueryStringAndFragment = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_cutQueryStringAndFragment = {description_cutQueryStringAndFragment, syntax_cutQueryStringAndFragment, arguments_cutQueryStringAndFragment, returned_value_cutQueryStringAndFragment, examples_cutQueryStringAndFragment, introduced_in_cutQueryStringAndFragment, category_cutQueryStringAndFragment};

    factory.registerFunction<FunctionCutQueryStringAndFragment>(documentation_cutQueryStringAndFragment);
}

}
