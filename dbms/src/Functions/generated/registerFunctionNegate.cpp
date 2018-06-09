#include "registerFunctionNegate.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

}
