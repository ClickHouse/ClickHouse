#include <Functions/registerFunctionLCM.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionLCM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLCM>();
}

}
