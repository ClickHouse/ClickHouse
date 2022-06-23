#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameNotILike
{
    static constexpr auto name = "notILike";
};

using NotILikeImpl = MatchImpl<true, true, /*case-insensitive*/true>;
using FunctionNotILike = FunctionsStringSearch<NotILikeImpl, NameNotILike>;

}

void registerFunctionNotILike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotILike>();
}
}
