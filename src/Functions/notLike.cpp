#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"


namespace DB
{

struct NameNotLike
{
    static constexpr auto name = "notLike";
};

using FunctionNotLike = FunctionsStringSearch<MatchImpl<true, true>, NameNotLike>;

void registerFunctionNotLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNotLike>();
}

}
