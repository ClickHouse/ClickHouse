#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchFirstPositionUTF8
{
    static constexpr auto name = "multiSearchFirstPositionUTF8";
};

using FunctionMultiSearchFirstPositionUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseSensitiveUTF8>, NameMultiSearchFirstPositionUTF8>;

void registerFunctionMultiSearchFirstPositionUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionUTF8>();
}

}
