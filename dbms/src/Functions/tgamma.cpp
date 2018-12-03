#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct TGammaName { static constexpr auto name = "tgamma"; };
using FunctionTGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<TGammaName, std::tgamma>>;

void registerFunctionTGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTGamma>();
}

}
