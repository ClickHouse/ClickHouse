#include "registerFunctionBitRotateLeft.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitRotateLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateLeft>();
}

}
