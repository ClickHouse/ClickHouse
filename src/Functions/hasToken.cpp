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
    factory.registerFunction<FunctionHasToken>(FunctionDocumentation
        {.description="Performs lookup of needle in haystack using tokenbf_v1 index."});

    factory.registerFunction<FunctionHasTokenOrNull>(FunctionDocumentation
        {.description="Performs lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed."});
}

}
