#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct AsinhName { static constexpr auto name = "asinh"; };
using FunctionAsinh = FunctionMathUnary<UnaryFunctionVectorized<AsinhName, asinh>>;

}

void registerFunctionAsinh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAsinh>();
}

}
