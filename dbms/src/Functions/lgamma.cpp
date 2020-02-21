#include <Functions/FunctionMathUnary.h>


namespace DB
{

struct LGammaName { static constexpr auto name = "lgamma"; };
using FunctionLGamma = FunctionMathUnary<UnaryFunctionPlain<LGammaName, std::lgamma>>;

void registerFunctionLGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLGamma>();
}

}
