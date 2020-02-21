#include <Functions/FunctionMathUnary.h>


namespace DB
{

struct TGammaName { static constexpr auto name = "tgamma"; };
using FunctionTGamma = FunctionMathUnary<UnaryFunctionPlain<TGammaName, std::tgamma>>;

void registerFunctionTGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTGamma>();
}

}
