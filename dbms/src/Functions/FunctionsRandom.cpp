#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

void registerFunctionsRandom(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRand>();
    factory.registerFunction<FunctionRand64>();
    factory.registerFunction<FunctionRandConstant>();
}

}
