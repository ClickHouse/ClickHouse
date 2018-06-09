#include <Functions/registerFunctionIntExp2.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionIntExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp2>();
}

}
