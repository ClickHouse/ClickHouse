#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasTokenImpl.h>

#include <Common/Volnitsky.h>

namespace DB
{

struct NameHasToken
{
    static constexpr auto name = "hasToken";
};

struct NameHasTokenOrNull
{
    static constexpr auto name = "hasTokenOrNull";
};

using FunctionHasToken
    = FunctionsStringSearch<HasTokenImpl<NameHasToken, Volnitsky, false>>;
using FunctionHasTokenOrNull
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenOrNull, Volnitsky, false>, ExecutionErrorPolicy::Null>;

REGISTER_FUNCTION(HasToken)
{
    factory.registerFunction<FunctionHasToken>(FunctionDocumentation{
        .description="Performs lookup of needle in haystack using tokenbf_v1 index.",
        .syntax="hasToken(haystack, token)",
        .arguments={
            {"haystack","String in which the search is performed. String or Enum."},
            {"token", "Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack)."}
        },
        .returned_value="Returns `1` if a given token is present in a haystack, or `0` otherwise.",
        .examples={
            {"Example", "SELECT hasToken('Hello World','Hello');", "1"}
        },
        .category=FunctionDocumentation::Category::StringSearch
        });

    factory.registerFunction<FunctionHasTokenOrNull>(FunctionDocumentation
        {
            .description="Performs lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed.",
            .syntax="hasToken(haystack, token)",
            .arguments={
                {"haystack","String in which the search is performed. String or Enum."},
                {"token", "Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack)."}
            },
            .returned_value="Returns `1` if the token is present in the haystack, `0` if it is not present, and `null` if the token is ill formed.",
            .examples={
                {"Example", "SELECT hasTokenOrNull('Hello World','Hello,World');", "null"}
            },
            .category=FunctionDocumentation::Category::StringSearch
        });
}

}
