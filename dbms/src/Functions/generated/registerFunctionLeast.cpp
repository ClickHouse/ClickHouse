#include "registerFunctionLeast.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeast>();
}

}
