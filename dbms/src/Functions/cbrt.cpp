#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct CbrtName { static constexpr auto name = "cbrt"; };
using FunctionCbrt = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CbrtName, cbrt>>;

void registerFunctionCbrt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCbrt>();
}

}
