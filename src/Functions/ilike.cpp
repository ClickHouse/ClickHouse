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

using ILikeImpl = MatchImpl<NameILike, true, false, /*case-insensitive*/true>;
using FunctionILike = FunctionsStringSearch<ILikeImpl>;

}

void registerFunctionILike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionILike>();
}

}
