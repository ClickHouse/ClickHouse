#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchAllPositions
{
    static constexpr auto name = "multiSearchAllPositions";
};

using FunctionMultiSearchAllPositions
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositions, PositionCaseSensitiveASCII>>;

}

void registerFunctionMultiSearchAllPositions(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchAllPositions>();
}

}
