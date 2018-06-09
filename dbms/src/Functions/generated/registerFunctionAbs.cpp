#include <Functions/registerFunctionAbs.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionAbs(FunctionFactory & factory)
{
    factory.registerFunction<FunctionAbs>();
}

}
