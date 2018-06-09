#include "registerFunctionIntExp2.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionIntExp2(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp2>();
}

}
