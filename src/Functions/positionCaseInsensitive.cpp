#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};

using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<PositionCaseInsensitiveASCII>, NamePositionCaseInsensitive>;

}

void registerFunctionPositionCaseInsensitive(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPositionCaseInsensitive>();
}
}
