#include <Functions/registerFunctionBitShiftLeft.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitShiftLeft(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftLeft>();
}

}
