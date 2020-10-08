#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{

struct NamePosition
{
    static constexpr auto name = "position";
};

using FunctionPosition = FunctionsStringSearch<PositionImpl<PositionCaseSensitiveASCII>, NamePosition>;

void registerFunctionPosition(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPosition>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("locate", NamePosition::name, FunctionFactory::CaseInsensitive);
}
}
