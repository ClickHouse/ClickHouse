#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryString.h>

namespace DB
{

struct NameQueryString { static constexpr auto name = "queryString"; };
using FunctionQueryString = FunctionStringToString<ExtractSubstringImpl<ExtractQueryString<true>>, NameQueryString>;

REGISTER_FUNCTION(QueryString)
{
    /// queryString documentation
    FunctionDocumentation::Description description_queryString = R"(
Returns the query string of a URL without the initial question mark, `#` and everything after `#`.
    )";
    FunctionDocumentation::Syntax syntax_queryString = "queryString(url)";
    FunctionDocumentation::Arguments arguments_queryString = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_queryString = {"Returns the query string of the URL without the initial question mark and fragment.", {"String"}};
    FunctionDocumentation::Examples examples_queryString = {
    {
        "Usage example",
        R"(
SELECT queryString('https://clickhouse.com/docs?query=value&param=123#section');
        )",
        R"(
┌─queryString(⋯3#section')─┐
│ query=value&param=123    │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_queryString = {1, 1};
    FunctionDocumentation::Category category_queryString = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_queryString = {description_queryString, syntax_queryString, arguments_queryString, returned_value_queryString, examples_queryString, introduced_in_queryString, category_queryString};

    factory.registerFunction<FunctionQueryString>(documentation_queryString);
}

}
