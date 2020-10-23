#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchFirstIndex
{
    static constexpr auto name = "multiSearchFirstIndex";
};

using FunctionMultiSearchFirstIndex
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseSensitiveASCII>, NameMultiSearchFirstIndex>;

void registerFunctionMultiSearchFirstIndex(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndex>();
}

}
