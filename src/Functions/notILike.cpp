#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>

namespace DB
{
namespace
{

struct NameNotILike
{
    static constexpr auto name = "notILike";
};

using NotILikeImpl = MatchImpl<NameNotILike, MatchTraits::Syntax::Like, MatchTraits::Case::Insensitive, MatchTraits::Result::Negate>;
using FunctionNotILike = FunctionsStringSearch<NotILikeImpl>;

}

REGISTER_FUNCTION(NotILike)
{
    FunctionDocumentation::Description description = "Checks whether a string does not match a pattern, case-insensitive. The pattern can contain special characters `%` and `_` for SQL LIKE matching.";
    FunctionDocumentation::Syntax syntax = "notILike(haystack, pattern)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "The input string to search in.", {"String", "FixedString"}},
        {"pattern", "The SQL LIKE pattern to match against. `%` matches any number of characters (including zero), `_` matches exactly one character.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string does not match the pattern (case-insensitive), otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT notILike('ClickHouse', '%house%');",
        R"(
┌─notILike('Cl⋯ '%house%')─┐
│                        0 │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionNotILike>(documentation);
}
}
