#include "registerFunctionGreatest.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionGreatest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatest>();
}

}
