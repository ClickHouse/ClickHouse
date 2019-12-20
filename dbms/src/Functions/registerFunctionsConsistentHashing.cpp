#include "registerFunctions.h"
namespace DB
{
void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    registerFunctionYandexConsistentHash(factory);
    registerFunctionJumpConsistentHash(factory);
    registerFunctionSumburConsistentHash(factory);
}

}
