#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct CosName { static constexpr auto name = "cos"; };
using FunctionCos = FunctionMathUnary<UnaryFunctionVectorized<CosName, cos>>;

void registerFunctionCos(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCos>(FunctionFactory::CaseInsensitive);
}

}
