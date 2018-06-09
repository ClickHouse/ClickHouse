#include "registerFunctionBitTest.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitTest(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTest>();
}

}
