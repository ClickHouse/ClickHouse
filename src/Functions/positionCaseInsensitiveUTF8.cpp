#include <Functions/FunctionsStringSearch.h>
#include <Functions/FunctionFactory.h>
#include <Functions/PositionImpl.h>


namespace DB
{
namespace
{

struct NamePositionCaseInsensitiveUTF8
{
    static constexpr auto name = "positionCaseInsensitiveUTF8";
};

using FunctionPositionCaseInsensitiveUTF8
    = FunctionsStringSearch<PositionImpl<NamePositionCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(PositionCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionPositionCaseInsensitiveUTF8>();
}

}
