#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsStringSearch.h>
#include <Functions/HasTokenImpl.h>

#include <Common/Volnitsky.h>

namespace DB
{

struct NameHasTokenCaseInsensitive
{
    static constexpr auto name = "hasTokenCaseInsensitive";
};

struct NameHasTokenCaseInsensitiveOrNull
{
    static constexpr auto name = "hasTokenCaseInsensitiveOrNull";
};

using FunctionHasTokenCaseInsensitive
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitive, VolnitskyCaseInsensitive, false>>;
using FunctionHasTokenCaseInsensitiveOrNull
    = FunctionsStringSearch<HasTokenImpl<NameHasTokenCaseInsensitiveOrNull, VolnitskyCaseInsensitive, false>, ExecutionErrorPolicy::Null>;

REGISTER_FUNCTION(HasTokenCaseInsensitive)
{
    factory.registerFunction<FunctionHasTokenCaseInsensitive>(
        FunctionDocumentation{
            .description="Performs case insensitive lookup of needle in haystack using tokenbf_v1 index.",
            .syntax="hasTokenCaseInsensitive(haystack, token)",
            .arguments={
                {"haystack", "String in which the search is performed. String or Enum."},
                {"token", "Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack)."}
            },
            .returned_value="Returns `1` if a given token is present in a haystack, `0` otherwise. Ignores case.",
            .examples={
                {"", "SELECT hasTokenCaseInsensitive('Hello World','hello');", "1"}
            },
            .category=FunctionDocumentation::Category::StringSearch
        },
        DB::FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionHasTokenCaseInsensitiveOrNull>(
        FunctionDocumentation{
            .description="Performs case insensitive lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed.",
            .syntax="hasTokenCaseInsensitiveOrNull(haystack, token)",
            .arguments={
                {"haystack", "String in which the search is performed. String or Enum."},
                {"token", "Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack)."}
            },
            .returned_value="1, if the token is present in the haystack, 0 if the token is not present, otherwise null if the token is ill-formed. UInt8.",
            .examples={
                {"Example", "SELECT hasTokenCaseInsensitiveOrNull('Hello World','hello,world');", "null"}
            },
            .category=FunctionDocumentation::Category::StringSearch
        },
        DB::FunctionFactory::Case::Insensitive
    );
}
}
