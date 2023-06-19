#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct CbrtName { static constexpr auto name = "cbrt"; };
using FunctionCbrt = FunctionMathUnary<UnaryFunctionVectorized<CbrtName, cbrt>>;

}

REGISTER_FUNCTION(Cbrt)
{
    factory.registerFunction<FunctionCbrt>();
}

}
