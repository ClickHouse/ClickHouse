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
            .description = R"(
Performs case insensitive lookup of needle in haystack using tokenbf_v1 index.

:::note
This function has certain pitfalls with non-default tokenizers and preprocessor or postprocessor expressions.
We recommend using [`hasAnyTokens`](#hasAnyTokens) and [`hasAllTokens`](#hasAllTokens) instead.
:::
            )",
            .syntax = "hasTokenCaseInsensitive(haystack, needle)",
            .introduced_in = {20, 1},
            .category = FunctionDocumentation::Category::StringSearch},
        DB::FunctionFactory::Case::Insensitive);

    factory.registerFunction<FunctionHasTokenCaseInsensitiveOrNull>(
        FunctionDocumentation{
            .description = R"(
Performs case insensitive lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed.

:::note
This function has certain pitfalls with non-default tokenizers and preprocessor or postprocessor expressions.
We recommend using [`hasAnyTokens`](#hasAnyTokens) and [`hasAllTokens`](#hasAllTokens) instead.
:::
            )",
            .syntax = "hasTokenCaseInsensitiveOrNull(haystack, needle)",
            .introduced_in = {23, 1},
            .category = FunctionDocumentation::Category::StringSearch},
        DB::FunctionFactory::Case::Insensitive);
}

}
