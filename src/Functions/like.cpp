#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "MatchImpl.h"


namespace DB
{

struct NameLike
{
    static constexpr auto name = "like";
};

using FunctionLike = FunctionsStringSearch<MatchImpl<true>, NameLike>;

void registerFunctionLike(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLike>();
}

}
