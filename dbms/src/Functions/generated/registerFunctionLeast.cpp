#include <Functions/registerFunctionLeast.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeast>();
}

}
