#include <Functions/registerFunctionGCD.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionGCD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGCD>();
}

}
