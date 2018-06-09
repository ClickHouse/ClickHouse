#include <Functions/registerFunctionGreatest.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatest>();
}

}
