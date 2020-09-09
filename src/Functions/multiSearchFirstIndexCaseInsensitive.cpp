#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstIndexImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstIndexCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstIndexCaseInsensitive";
};

using FunctionMultiSearchFirstIndexCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstIndexImpl<PositionCaseInsensitiveASCII>, NameMultiSearchFirstIndexCaseInsensitive>;

}

void registerFunctionMultiSearchFirstIndexCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstIndexCaseInsensitive>();
}

}
