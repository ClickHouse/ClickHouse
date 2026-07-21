#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


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

REGISTER_FUNCTION(PositionUTF8)
{
    factory.registerFunction<FunctionPositionUTF8>();
}

}
