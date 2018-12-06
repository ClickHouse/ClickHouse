#include <Functions/FunctionMathUnaryFloat64.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

struct AsinName { static constexpr auto name = "asin"; };
using FunctionAsin = FunctionMathUnaryFloat64<UnaryFunctionVectorized<AsinName, asin>>;

void registerFunctionAsin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAsin>(FunctionFactory::CaseInsensitive);
}

}
