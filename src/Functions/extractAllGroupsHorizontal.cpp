#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct HorizontalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::HORIZONTAL;
    static constexpr auto Name = "extractAllGroupsHorizontal";
};

}

namespace DB
{

REGISTER_FUNCTION(ExtractAllGroupsHorizontal)
{
    FunctionDocumentation::Description description = R"(
Matches all groups of a string using the provided regular expression and returns an array of arrays, where each array contains all captures from the same capturing group, organized by group number.
)";
    FunctionDocumentation::Syntax syntax = "extractAllGroupsHorizontal(s, regexp)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string to extract from.", {"String", "FixedString"}},
        {"regexp", "Regular expression to match by.", {"const String", "const FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of arrays, where each inner array contains all captures from one capturing group across all matches. The first inner array contains all captures from group 1, the second from group 2, etc. If no matches are found, returns an empty array.", {"Array(Array(String))"}};
    FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
WITH '< Server: nginx
< Date: Tue, 22 Jan 2019 00:26:14 GMT
< Content-Type: text/html; charset=UTF-8
< Connection: keep-alive
' AS s
SELECT extractAllGroupsHorizontal(s, '< ([\\w\\-]+): ([^\\r\\n]+)');
)",
            R"(
[['Server','Date','Content-Type','Connection'],['nginx','Tue, 22 Jan 2019 00:26:14 GMT','text/html; charset=UTF-8','keep-alive']]
    )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionExtractAllGroups<HorizontalImpl>>(documentation);
}

}
