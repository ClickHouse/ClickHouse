#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct CosName { static constexpr auto name = "cos"; };
using FunctionCos = FunctionMathUnaryFloat64<UnaryFunctionVectorized<CosName, cos>>;

void registerFunction(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCos>(FunctionFactory::CaseInsensitive);
}

}
