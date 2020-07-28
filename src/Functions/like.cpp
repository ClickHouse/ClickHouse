#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"


namespace DB
{

struct NameLike
{
    static constexpr auto name = "like";
};

namespace
{
    using LikeImpl = MatchImpl</*SQL LIKE */ true, /*revert*/false>;
}

using FunctionLike = FunctionsStringSearch<LikeImpl, NameLike>;

void registerFunctionLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLike>();
}
}
