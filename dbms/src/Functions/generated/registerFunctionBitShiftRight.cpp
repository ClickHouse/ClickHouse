#include "registerFunctionBitShiftRight.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitShiftRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

}
