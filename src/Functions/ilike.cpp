#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MatchImpl.h>

namespace DB
{
namespace
{

struct NameILike
{
    static constexpr auto name = "ilike";
};

using ILikeImpl = MatchImpl<NameILike, MatchTraits::Syntax::Like, MatchTraits::Case::Insensitive, MatchTraits::Result::DontNegate>;
using FunctionILike = FunctionsStringSearch<ILikeImpl>;

}

REGISTER_FUNCTION(ILike)
{
    FunctionDocumentation::Description description = "Like [`like`](#like) but searches case-insensitively. Supports the optional `ESCAPE` clause (see `like`).";
    FunctionDocumentation::Syntax syntax = R"(
ilike(haystack, pattern[, escape_character])
-- haystack ILIKE pattern [ESCAPE 'escape_character']
    )";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String in which the search is performed.", {"String", "FixedString"}},
        {"pattern", "LIKE pattern to match against.", {"String"}},
        {"escape_character", "Optional single-character string to use as the escape character instead of `\\`. Default: `\\`.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the string matches the LIKE pattern (case-insensitive), otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "Usage example",
        "SELECT ilike('ClickHouse', '%house%');",
        R"(
┌─ilike('ClickHouse', '%house%')─┐
│                              1 │
└────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionILike>(documentation);
}

}
