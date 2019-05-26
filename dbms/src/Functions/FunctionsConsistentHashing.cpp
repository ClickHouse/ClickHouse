#include "FunctionsConsistentHashing.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionsConsistentHashing(FunctionFactory & factory)
{
    factory.registerFunction<FunctionYandexConsistentHash>();
    factory.registerFunction<FunctionJumpConsistentHash>();
    factory.registerFunction<FunctionSumburConsistentHash>();
}

}
