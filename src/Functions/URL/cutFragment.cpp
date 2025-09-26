#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/fragment.h>

namespace DB
{

struct NameCutFragment { static constexpr auto name = "cutFragment"; };
using FunctionCutFragment = FunctionStringToString<CutSubstringImpl<ExtractFragment<false>>, NameCutFragment>;

REGISTER_FUNCTION(CutFragment)
{
    /// cutFragment documentation
    FunctionDocumentation::Description description_cutFragment = R"(
Removes the fragment identifier, including the number sign, from a URL.
    )";
    FunctionDocumentation::Syntax syntax_cutFragment = "cutFragment(url)";
    FunctionDocumentation::Arguments arguments_cutFragment = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_cutFragment = {"Returns the URL with fragment identifier removed.", {"String"}};
    FunctionDocumentation::Examples examples_cutFragment = {
    {
        "Usage example",
        R"(
SELECT cutFragment('http://example.com/path?query=value#fragment123');
        )",
        R"(
┌─cutFragment('http://example.com/path?query=value#fragment123')─┐
│ http://example.com/path?query=value                            │
└────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_cutFragment = {1, 1};
    FunctionDocumentation::Category category_cutFragment = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation_cutFragment = {description_cutFragment, syntax_cutFragment, arguments_cutFragment, returned_value_cutFragment, examples_cutFragment, introduced_in_cutFragment, category_cutFragment};

    factory.registerFunction<FunctionCutFragment>(documentation_cutFragment);
}

}
