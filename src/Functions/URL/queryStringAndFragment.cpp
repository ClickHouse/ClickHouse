#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryStringAndFragment.h>

namespace DB
{

struct NameQueryStringAndFragment { static constexpr auto name = "queryStringAndFragment"; };
using FunctionQueryStringAndFragment = FunctionStringToString<ExtractSubstringImpl<ExtractQueryStringAndFragment<true>>, NameQueryStringAndFragment>;

REGISTER_FUNCTION(QueryStringAndFragment)
{
    /// queryStringAndFragment documentation
    FunctionDocumentation::Description description_queryStringAndFragment = R"(
Returns the query string and fragment identifier of a URL.
    )";
    FunctionDocumentation::Syntax syntax_queryStringAndFragment = "queryStringAndFragment(url)";
    FunctionDocumentation::Arguments arguments_queryStringAndFragment = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_queryStringAndFragment = {"Returns the query string and fragment identifier of the URL.", {"String"}};
    FunctionDocumentation::Examples examples_queryStringAndFragment = {
    {
        "Usage example",
        R"(
SELECT queryStringAndFragment('https://clickhouse.com/docs?query=value&param=123#section');
        )",
        R"(
┌─queryStringAnd⋯=123#section')─┐
│ query=value&param=123#section │
└───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_queryStringAndFragment = {1, 1};
    FunctionDocumentation::Category category_queryStringAndFragment = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_queryStringAndFragment = {description_queryStringAndFragment, syntax_queryStringAndFragment, arguments_queryStringAndFragment, returned_value_queryStringAndFragment, examples_queryStringAndFragment, introduced_in_queryStringAndFragment, category_queryStringAndFragment};

    factory.registerFunction<FunctionQueryStringAndFragment>(documentation_queryStringAndFragment);
}

}
