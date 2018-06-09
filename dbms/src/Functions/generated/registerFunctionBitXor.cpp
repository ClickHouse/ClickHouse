#include "registerFunctionBitXor.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitXor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitXor>();
}

}
