#include "registerFunctionLCM.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionLCM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLCM>();
}

}
