#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"

namespace DB
{
namespace
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

using FunctionNotLike = FunctionsStringSearch<MatchImpl<NameNotLike, true, true>>;

}

void registerFunctionNotLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotLike>();
}

}
