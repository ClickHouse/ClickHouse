#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct SinName { static constexpr auto name = "sin"; };
using FunctionSin = FunctionMathUnary<UnaryFunctionVectorized<SinName, sin>>;

void registerFunctionSin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSin>(FunctionFactory::CaseInsensitive);
}

}
