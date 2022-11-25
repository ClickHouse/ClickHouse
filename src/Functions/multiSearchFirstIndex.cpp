#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndex
{
    static constexpr auto name = "multiSearchFirstIndex";
};

using FunctionMultiSearchFirstIndex
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<NameMultiSearchFirstIndex, PositionCaseSensitiveASCII>>;

}

void registerFunctionMultiSearchFirstIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndex>();
}

}
