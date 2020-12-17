#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameILike
{
    static constexpr auto name = "ilike";
};

using ILikeImpl = MatchImpl<true, false, /*case-insensitive*/true>;
using FunctionILike = FunctionsStringSearch<ILikeImpl, NameILike>;

}

void registerFunctionILike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionILike>();
}

}
