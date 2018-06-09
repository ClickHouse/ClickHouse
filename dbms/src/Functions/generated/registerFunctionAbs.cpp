#include "registerFunctionAbs.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionAbs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAbs>();
}

}
