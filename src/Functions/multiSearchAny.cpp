#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchImpl.h"
#include "PositionImpl.h"


namespace DB
{

struct NameMultiSearchAny
{
    static constexpr auto name = "multiSearchAny";
};

using FunctionMultiSearch = FunctionsMultiStringSearch<MultiSearchImpl<PositionCaseSensitiveASCII>, NameMultiSearchAny>;

void registerFunctionMultiSearchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearch>();
}

}
