#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"


namespace DB
{

struct NameLike
{
    static constexpr auto name = "like";
};

struct NameILike
{
    static constexpr auto name = "ilike";
};

namespace
{
    using LikeImpl = MatchImpl</*SQL LIKE */ true, /*revert*/false>;
    using ILikeImpl = MatchImpl<true, false, /*case-insensitive*/true>;
}

using FunctionLike = FunctionsStringSearch<LikeImpl, NameLike>;
using FunctionIlike = FunctionsStringSearch<ILikeImpl, NameILike>;

void registerFunctionLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLike>();
}

void registerFunctionIlike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIlike>();
}

}
