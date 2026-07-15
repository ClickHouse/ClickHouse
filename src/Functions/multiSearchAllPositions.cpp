#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


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

REGISTER_FUNCTION(MultiSearchAllPositions)
{
    factory.registerFunction<FunctionMultiSearchAllPositions>();
}

}
