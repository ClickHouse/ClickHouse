#include <Functions/registerFunctionBitNot.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

}
