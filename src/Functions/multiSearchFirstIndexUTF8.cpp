#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multiSearchFirstIndexUTF8";
};

using FunctionMultiSearchFirstIndexUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseSensitiveUTF8>, NameMultiSearchFirstIndexUTF8>;

void registerFunctionMultiSearchFirstIndexUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>();
}

}
