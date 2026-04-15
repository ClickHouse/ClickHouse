#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


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

REGISTER_FUNCTION(Position)
{
    factory.registerFunction<FunctionPosition>({}, FunctionFactory::Case::Insensitive);
}
}
