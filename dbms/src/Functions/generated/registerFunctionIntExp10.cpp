#include "registerFunctionIntExp10.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionIntExp10(FunctionFactory & factory)
{
    factory.registerFunction<FunctionIntExp10>();
}

}
