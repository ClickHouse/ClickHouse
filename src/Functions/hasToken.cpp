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
    FunctionDocumentation::Description description = R"(
Checks if the given token is present in the haystack.

A token is defined as the longest possible sub-sequence of consecutive characters `[0-9A-Za-z_]`, i.e. numbers, ASCII letters and underscore.
    )";
    FunctionDocumentation::Syntax syntax = "hasToken(haystack, token)";
    FunctionDocumentation::Arguments arguments = {
        {"haystack", "String to be searched.", {"String"}},
        {"token", "Token to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the token is found, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Token search",
        "SELECT hasToken('clickhouse test', 'test')",
        R"(
┌─hasToken('clickhouse test', 'test')─┐
│                                   1 │
└─────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    FunctionDocumentation::Description description_or_null = R"(
Like [`hasToken`](#hasToken) but returns null if token is ill-formed.
    )";
    FunctionDocumentation::Syntax syntax_or_null = "hasTokenOrNull(haystack, token)";
    FunctionDocumentation::Arguments arguments_or_null = {
        {"haystack", "String to be searched. Must be constant.", {"String"}},
        {"token", "Token to search for.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_or_null = {"Returns `1` if the token is found, `0` otherwise, null if token is ill-formed.", {"Nullable(UInt8)"}};
    FunctionDocumentation::Examples examples_or_null = {
    {
        "Usage example",
        "SELECT hasTokenOrNull('apple banana cherry', 'ban ana');",
        R"(
┌─hasTokenOrNu⋯ 'ban ana')─┐
│                     ᴺᵁᴸᴸ │
└──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_or_null = {20, 1};
    FunctionDocumentation::Category category_or_null = FunctionDocumentation::Category::StringSearch;
    FunctionDocumentation documentation_or_null = {description_or_null, syntax_or_null, arguments_or_null, returned_value_or_null, examples_or_null, introduced_in_or_null, category_or_null};

    factory.registerFunction<FunctionHasToken>(documentation);
    factory.registerFunction<FunctionHasTokenOrNull>(documentation_or_null);
}

}
