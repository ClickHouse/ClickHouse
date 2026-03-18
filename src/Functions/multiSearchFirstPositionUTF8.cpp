#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionUTF8
{
    static constexpr auto name = "multiSearchFirstPositionUTF8";
};

using FunctionMultiSearchFirstPositionUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<NameMultiSearchFirstPositionUTF8, PositionCaseSensitiveUTF8>>;

}

REGISTER_FUNCTION(MultiSearchFirstPositionUTF8)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionUTF8>();
}

}
