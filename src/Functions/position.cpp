#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "PositionImpl.h"


namespace DB
{
namespace
{

struct NamePosition
{
    static constexpr auto name = "position";
};

using FunctionPosition = FunctionsStringSearch<PositionImpl<NamePosition, PositionCaseSensitiveASCII>>;

}

void registerFunctionPosition(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPosition>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("locate", NamePosition::name, FunctionFactory::CaseInsensitive);
}
}
