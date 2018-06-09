#include <Functions/registerFunctionNegate.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionNegate(FunctionFactory & factory)
{
    factory.registerFunction<FunctionNegate>();
}

}
