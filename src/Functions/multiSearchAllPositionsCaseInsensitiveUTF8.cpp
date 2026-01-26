#include <Functions/FunctionsMultiStringPosition.h>
#include <Functions/FunctionFactory.h>
#include <Functions/MultiSearchAllPositionsImpl.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NameMultiSearchAllPositionsCaseInsensitiveUTF8
{
    static constexpr auto name = "multiSearchAllPositionsCaseInsensitiveUTF8";
};

using FunctionMultiSearchAllPositionsCaseInsensitiveUTF8
    = FunctionsMultiStringPosition<MultiSearchAllPositionsImpl<NameMultiSearchAllPositionsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchAllPositionsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionMultiSearchAllPositionsCaseInsensitiveUTF8>();
}

}
