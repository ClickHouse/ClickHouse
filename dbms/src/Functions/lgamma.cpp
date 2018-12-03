#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct LGammaName { static constexpr auto name = "lgamma"; };
using FunctionLGamma = FunctionMathUnaryFloat64<UnaryFunctionPlain<LGammaName, std::lgamma>>;

void registerFunctionLGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLGamma>();
}

}
