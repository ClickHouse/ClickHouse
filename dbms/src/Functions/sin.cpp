#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct SinName { static constexpr auto name = "sin"; };
using FunctionSin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<SinName, sin>>;

void registerFunctionSin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSin>(FunctionFactory::CaseInsensitive);
}

}
