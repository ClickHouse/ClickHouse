#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct ErfName { static constexpr auto name = "erf"; };
using FunctionErf = FunctionMathUnary<UnaryFunctionPlain<ErfName, std::erf>>;

void registerFunctionErf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErf>();
}

}
