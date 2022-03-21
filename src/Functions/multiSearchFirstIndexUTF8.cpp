#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexUTF8
{
    static constexpr auto name = "multiSearchFirstIndexUTF8";
};

using FunctionMultiSearchFirstIndexUTF8
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndexUTF8, PositionCaseSensitiveUTF8>>;

}

void registerFunctionMultiSearchFirstIndexUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexUTF8>();
}

}
