#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace DB
{

REGISTER_FUNCTION(ExtractAllGroupsVertical)
{
    FunctionDocumentation::Description description = R"(
Matches all groups of a string using a regular expression and returns an array of arrays, where each array includes matching fragments from every group, grouped in order of appearance in the input string.
)";
    FunctionDocumentation::Syntax syntax = "extractAllGroupsVertical(s, regexp)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string to extract from.", {"String", "FixedString"}},
        {"regexp", "Regular expression to match by.", {"const String", "const FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of arrays, where each inner array contains the captured groups from one match. Each match produces an array with elements corresponding to the capturing groups in the regular expression (group 1, group 2, etc.). If no matches are found, returns an empty array.", {"Array(Array(String))"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
WITH '< Server: nginx
< Date: Tue, 22 Jan 2019 00:26:14 GMT
< Content-Type: text/html; charset=UTF-8
< Connection: keep-alive
' AS s
SELECT extractAllGroupsVertical(s, '< ([\\w\\-]+): ([^\\r\\n]+)');
)",
        R"(
[['Server','nginx'],['Date','Tue, 22 Jan 2019 00:26:14 GMT'],['Content-Type','text/html; charset=UTF-8'],['Connection','keep-alive']]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("extractAllGroupsVertical", [](ContextPtr ctx){ return FunctionExtractAllGroups::create(ctx, "extractAllGroupsVertical", ExtractAllGroupsResultKind::VERTICAL); }, documentation);
    factory.registerAlias("extractAllGroups", "extractAllGroupsVertical");
}

}
