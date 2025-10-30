#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryString.h>

namespace DB
{

struct NameCutQueryString { static constexpr auto name = "cutQueryString"; };
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;

REGISTER_FUNCTION(CutQueryString)
{
    /// cutQueryString documentation
    FunctionDocumentation::Description description_cutQueryString = R"(
Removes the query string, including the question mark from a URL.
    )";
    FunctionDocumentation::Syntax syntax_cutQueryString = "cutQueryString(url)";
    FunctionDocumentation::Arguments arguments_cutQueryString = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cutQueryString = {"Returns the URL with query string removed.", {"String"}};
    FunctionDocumentation::Examples examples_cutQueryString = {
    {
        "Usage example",
        R"(
SELECT cutQueryString('http://example.com/path?query=value&param=123#fragment');
        )",
        R"(
┌─cutQueryString('http://example.com/path?query=value&param=123#fragment')─┐
│ http://example.com/path#fragment                                         │
└──────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cutQueryString = {1, 1};
    FunctionDocumentation::Category category_cutQueryString = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_cutQueryString = {description_cutQueryString, syntax_cutQueryString, arguments_cutQueryString, returned_value_cutQueryString, examples_cutQueryString, introduced_in_cutQueryString, category_cutQueryString};

    factory.registerFunction<FunctionCutQueryString>(documentation_cutQueryString);
}

}
