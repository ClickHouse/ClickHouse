#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace
{

struct CoshName { static constexpr auto name = "cosh"; };
using FunctionCosh = FunctionMathUnary<UnaryFunctionVectorized<CoshName, cosh>>;

}

void registerFunctionCosh(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCosh>();
}

}
