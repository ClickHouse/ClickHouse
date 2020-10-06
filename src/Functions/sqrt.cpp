#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct SqrtName { static constexpr auto name = "sqrt"; };
using FunctionSqrt = FunctionMathUnary<UnaryFunctionVectorized<SqrtName, sqrt>>;

void registerFunctionSqrt(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSqrt>(FunctionFactory::CaseInsensitive);
}

}
