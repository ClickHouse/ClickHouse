#include <Functions/registerFunctionBitShiftRight.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitShiftRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitShiftRight>();
}

}
