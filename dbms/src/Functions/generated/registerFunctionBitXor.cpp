#include <Functions/registerFunctionBitXor.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitXor(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitXor>();
}

}
