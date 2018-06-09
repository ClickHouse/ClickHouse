#include <Functions/registerFunctionBitTestAll.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

}
