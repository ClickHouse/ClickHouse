#include <Functions/registerFunctionBitRotateRight.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitRotateRight(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitRotateRight>();
}

}
