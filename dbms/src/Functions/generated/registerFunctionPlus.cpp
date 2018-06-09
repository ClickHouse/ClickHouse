#include "registerFunctionPlus.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionPlus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPlus>();
}

}
