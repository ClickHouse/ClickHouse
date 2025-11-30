#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>

namespace DB
{
namespace
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

using NotLikeImpl = MatchImpl<NameNotLike, MatchTraits::Syntax::Like, MatchTraits::Case::Sensitive, MatchTraits::Result::Negate>;
using FunctionNotLike = FunctionsStringSearch<NotLikeImpl>;

}

REGISTER_FUNCTION(NotLike)
{
    FunctionDocumentation::Description description = "Similar to [`like`](#like) but negates the result.";
    FunctionDocumentation::Syntax syntax = R"(
notLike(haystack, pattern)
-- haystack NOT LIKE pattern
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "LIKE pattern to match against.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string does not match the `LIKE` pattern, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT notLike('ClickHouse', '%House%');",
        R"(
┌─notLike('Cli⋯ '%House%')─┐
│                        0 │
└──────────────────────────┘
        )"
    },
    {
        "Non-matching pattern",
        "SELECT notLike('ClickHouse', '%SQL%');",
        R"(
┌─notLike('Cli⋯', '%SQL%')─┐
│                        1 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNotLike>(documentation);
}

}
