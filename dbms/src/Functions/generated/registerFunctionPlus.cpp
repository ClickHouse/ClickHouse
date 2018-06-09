#include <Functions/registerFunctionPlus.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionPlus(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPlus>();
}

}
