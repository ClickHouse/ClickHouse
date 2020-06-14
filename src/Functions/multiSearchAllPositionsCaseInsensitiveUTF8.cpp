#include "FunctionsMultiStringPosition.h"
#include "FunctionFactory.h"
#include "MultiSearchAllPositionsImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitiveUTF8";
};

using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8 = FunctionsMultiStringPosition<
    MultiSearchAllPositionsImpl<PositionCaseInsensitiveUTF8>,
    NameMultiSearchAllPositionsCaseInsensitiveUTF8>;

void registerFunctionMultiSearchAllPositionsCaseInsensitiveUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>();
}

}
