#include "registerFunctions.h"
namespace DB
{
void registerFunctionsRandom(FunctionFactory & factory)
{
    registerFunctionRand(factory);
    registerFunctionRand64(factory);
    registerFunctionRandConstant(factory);
    registerFunctionGenerateUUIDv4(factory);
}

}
