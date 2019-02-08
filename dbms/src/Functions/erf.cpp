#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct ErfName { static constexpr auto name = "erf"; };
using FunctionErf = FunctionMathUnaryFloat64<UnaryFunctionPlain<ErfName, std::erf>>;

void registerFunctionErf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErf>();
}

}
