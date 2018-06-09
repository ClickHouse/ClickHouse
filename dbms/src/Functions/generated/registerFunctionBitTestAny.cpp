#include <Functions/registerFunctionBitTestAny.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

}
