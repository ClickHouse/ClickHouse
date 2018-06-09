#include "registerFunctionBitShiftLeft.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitShiftLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftLeft>();
}

}
