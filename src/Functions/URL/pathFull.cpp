#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/StringHelpers.h>
#include <Functions/URL/path.h>
#include <base/find_symbols.h>

namespace DB
{

struct NamePathFull { static constexpr auto name = "pathFull"; };
using FunctionPathFull = FunctionStringToString<ExtractSubstringImpl<ExtractPath<true>>, NamePathFull>;

REGISTER_FUNCTION(PathFull)
{
    /// pathFull documentation
    FunctionDocumentation::Description description_pathFull = R"(
The same as [`path`](#path), but includes the query string and fragment of the URL.
    )";
    FunctionDocumentation::Syntax syntax_pathFull = "pathFull(url)";
    FunctionDocumentation::Arguments arguments_pathFull = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_pathFull = {"Returns the path of the URL including query string and fragment.", {"String"}};
    FunctionDocumentation::Examples examples_pathFull = {
    {
        "Usage example",
        R"(
SELECT pathFull('https://clickhouse.com/docs/sql-reference/functions/url-functions/?query=value#section');
        )",
        R"(
┌─pathFull('https://clickhouse.com⋯unctions/?query=value#section')─┐
│ /docs/sql-reference/functions/url-functions/?query=value#section │
└──────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_pathFull = {1, 1};
    FunctionDocumentation::Category category_pathFull = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_pathFull = {description_pathFull, syntax_pathFull, arguments_pathFull, returned_value_pathFull, examples_pathFull, introduced_in_pathFull, category_pathFull};

    factory.registerFunction<FunctionPathFull>(documentation_pathFull);
}

}
