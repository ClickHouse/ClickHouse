#include "registerFunctionBitTestAll.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitTestAll(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitTestAll>();
}

}
