#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchAllPositionsCaseInsensitive
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitive";
};

using FunctionMultiSearchAllPositionsCaseInsensitive
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<PositionCaseInsensitiveASCII>, NameMultiSearchAllPositionsCaseInsensitive>;

void registerFunctionMultiSearchAllPositionsCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitive>();
}

}
