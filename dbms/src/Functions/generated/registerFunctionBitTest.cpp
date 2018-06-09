#include <Functions/registerFunctionBitTest.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTest>();
}

}
