#include "registerFunctionBitTestAny.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitTestAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAny>();
}

}
