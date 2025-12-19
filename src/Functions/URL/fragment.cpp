#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/fragment.h>

namespace DB
{

struct NameFragment { static constexpr auto name = "fragment"; };
using FunctionFragment = FunctionStringToString<ExtractSubstringImpl<ExtractFragment<true>>, NameFragment>;

REGISTER_FUNCTION(Fragment)
{
    /// fragment documentation
    FunctionDocumentation::Description description_fragment = R"(
Returns the fragment identifier without the initial hash symbol.
    )";
    FunctionDocumentation::Syntax syntax_fragment = "fragment(url)";
    FunctionDocumentation::Arguments arguments_fragment = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fragment = {"Returns the fragment identifier without the initial hash symbol.", {"String"}};
    FunctionDocumentation::Examples examples_fragment = {
        {
            "Usage example",
            R"(
SELECT fragment('https://clickhouse.com/docs/getting-started/quick-start/cloud#1-create-a-clickhouse-service');
            )",
            R"(
┌─fragment('http⋯ouse-service')─┐
│ 1-create-a-clickhouse-service │
└───────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in_fragment = {1, 1};
    FunctionDocumentation::Category category_fragment = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_fragment = {description_fragment, syntax_fragment, arguments_fragment, returned_value_fragment, examples_fragment, introduced_in_fragment, category_fragment};

    factory.registerFunction<FunctionFragment>(documentation_fragment);
}

}
