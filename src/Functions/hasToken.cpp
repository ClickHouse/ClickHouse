#include "FunctionFactory.h"
#include "FunctionsStringSearch.h"
#include "HasTokenImpl.h"

#include <Common/Volnitsky.h>

namespace
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
}

REGISTER_FUNCTION(HasToken)
{
    factory.registerFunction<FunctionHasToken>(
        {"Performs lookup of needle in haystack using tokenbf_v1 index."}, DB::FunctionFactory::CaseSensitive);

    factory.registerFunction<FunctionHasTokenOrNull>(
        {"Performs lookup of needle in haystack using tokenbf_v1 index. Returns null if needle is ill-formed."},
        DB::FunctionFactory::CaseSensitive);
}
