#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct CbrtName { static constexpr auto name = "cbrt"; };
using FunctionCbrt = FunctionMathUnary<UnaryFunctionVectorized<CbrtName, cbrt>>;

void registerFunctionCbrt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCbrt>();
}

}
