#include "FunctionsMultiStringSearch.h"
#include "FunctionFactory.h"
#include "MultiSearchFirstPositionImpl.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NameMultiSearchFirstPositionCaseInsensitive
{
    static constexpr auto name = "multiSearchFirstPositionCaseInsensitive";
};

using FunctionMultiSearchFirstPositionCaseInsensitive
    = FunctionsMultiStringSearch<MultiSearchFirstPositionImpl<PositionCaseInsensitiveASCII>, NameMultiSearchFirstPositionCaseInsensitive>;

}

void registerFunctionMultiSearchFirstPositionCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiSearchFirstPositionCaseInsensitive>();
}

}
