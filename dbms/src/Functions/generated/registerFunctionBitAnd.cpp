#include "registerFunctionBitAnd.h"

#include <Functions/FunctionFactory.h>
#include "FunctionsArithmetic.h"

namespace DB
{

void registerFunctionBitAnd(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBitAnd>();
}

}
