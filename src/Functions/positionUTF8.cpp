#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{

struct NamePositionUTF8
{
    static constexpr auto name = "positionUTF8";
};

using FunctionPositionUTF8 = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveUTF8>, NamePositionUTF8>;

void registerFunctionPositionUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPositionUTF8>();
}

}
