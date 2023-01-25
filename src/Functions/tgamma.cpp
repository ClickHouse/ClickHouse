#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct TGammaName { static constexpr auto name = "tgamma"; };
using FunctionTGamma = FunctionMathUnary<UnaryFunctionVectorized<TGammaName, std::tgamma>>;

}

void registerFunctionTGamma(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTGamma>();
}

}
