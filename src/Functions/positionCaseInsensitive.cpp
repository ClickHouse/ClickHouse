#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePositionCaseInsensitive
{
    static constexpr auto name = "positionCaseInsensitive";
};

using FunctionPositionCaseInsensitive = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(PositionCaseInsensitive)
{
    factory.registerFunction<FunctionPositionCaseInsensitive>();
    factory.registerAlias("instr", NamePositionCaseInsensitive::name, FunctionFactory::Case::Insensitive);
}
}
