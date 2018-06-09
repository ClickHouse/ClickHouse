#include "registerFunctionBitRotateRight.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitRotateRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateRight>();
}

}
