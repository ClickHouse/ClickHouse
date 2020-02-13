#include <Functions/FunctionMathUnary.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

struct AsinName { static constexpr auto name = "asin"; };
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, asin>>;

void registerFunctionAsin(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAsin>(FunctionFactory::CaseInsensitive);
}

}
