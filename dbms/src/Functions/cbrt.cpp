#include <Functions/FunctionMathUnary.h>


namespace DB
{

struct CbrtName { static constexpr auto name = "cbrt"; };
using FunctionCbrt = FunctionMathUnary<UnaryFunctionVectorized<CbrtName, cbrt>>;

void registerFunctionCbrt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCbrt>();
}

}
