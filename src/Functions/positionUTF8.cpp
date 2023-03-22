#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};

using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<NamePositionUTF8, PositionCaseSensitiveUTF8>>;

}

void registerFunctionPositionUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPositionUTF8>();
}

}
