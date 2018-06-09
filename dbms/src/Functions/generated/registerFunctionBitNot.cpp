#include "registerFunctionBitNot.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitNot(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitNot>();
}

}
