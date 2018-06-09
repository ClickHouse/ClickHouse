#include <Functions/registerFunctionIntExp10.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsArithmetic.h>

namespace DB
{

void registerFunctionIntExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp10>();
}

}
