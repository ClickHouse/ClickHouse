#include "registerFunctionGCD.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionGCD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGCD>();
}

}
