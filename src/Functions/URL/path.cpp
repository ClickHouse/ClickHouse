#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>
#include <Functions/URL/path.h>
#include <base/find_symbols.h>


namespace DB
{

struct NamePath { static constexpr auto name = "path"; };
using FunctionPath = FunctionStringToString<ExtractSubstringImpl<ExtractPath<false>>, NamePath>;

REGISTER_FUNCTION(Path)
{
    /// path documentation
    FunctionDocumentation::Description description_path = R"(
Returns the path without query string from a URL.
    )";
    FunctionDocumentation::Syntax syntax_path = "path(url)";
    FunctionDocumentation::Arguments arguments_path = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_path = {"Returns the path of the URL without query string.", {"String"}};
    FunctionDocumentation::Examples examples_path = {
    {
        "Usage example",
        R"(
SELECT path('https://clickhouse.com/docs/sql-reference/functions/url-functions/?query=value');
        )",
        R"(
┌─path('https://clickhouse.com/en/sql-reference/functions/url-functions/?query=value')─┐
│ /docs/sql-reference/functions/url-functions/                                         │
└──────────────────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_path = {1, 1};
    FunctionDocumentation::Category category_path = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_path = {description_path, syntax_path, arguments_path, returned_value_path, examples_path, introduced_in_path, category_path};

    factory.registerFunction<FunctionPath>(documentation_path);
}

}
