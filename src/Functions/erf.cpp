#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct ErfName { static constexpr auto name = "erf"; };
using FunctionErf = FunctionMathUnary<UnaryFunctionVectorized<ErfName, std::erf>>;

}

void registerFunctionErf(FunctionFactory & factory)
{
    factory.registerFunction<FunctionErf>();
}

}
