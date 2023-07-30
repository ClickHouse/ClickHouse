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

using FunctionHasToken = DB::FunctionsStringSearch<DB::HasTokenImpl<NameHasToken, DB::VolnitskyCaseSensitiveToken, false>>;
using FunctionHasTokenOrNull = DB::
    FunctionsStringSearch<DB::HasTokenImpl<NameHasTokenOrNull, DB::VolnitskyCaseSensitiveToken, false>, DB::ExecutionErrorPolicy::Null>;

REGISTER_FUNCTION(HasToken)
{
    factory.registerFunction<FunctionHasToken>(FunctionDocumentation
        {.description="Performs lookup of needle in haystack using tokenbf_v1 index."}, DB::FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionHasTokenOrNull>(FunctionDocumentation
        {.description="Performs lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed."},
        DB::FunctionFactory::CaseSensitive);
}

}
